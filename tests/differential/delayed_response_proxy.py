#!/usr/bin/env python3
"""Forward RocketMQ TCP frames while delaying the next Broker response."""

import argparse
import socket
import socketserver
import threading
import time
from pathlib import Path


def recv_exact(connection, count):
    chunks = []
    remaining = count
    while remaining:
        chunk = connection.recv(remaining)
        if not chunk:
            if not chunks:
                return None
            raise ConnectionError("peer closed during a RocketMQ frame")
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def log(server, event, connection_id):
    with server.log_lock:
        with server.log_path.open("a", encoding="utf-8") as output:
            output.write(f"PROXY\t{event}\t{connection_id}\n")


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        connection_id = f"{self.client_address[1]}-{time.monotonic_ns()}"
        try:
            upstream = socket.create_connection(self.server.upstream, timeout=5)
        except OSError as error:
            log(self.server, f"CONNECT_ERROR:{type(error).__name__}", connection_id)
            return

        def forward_requests():
            try:
                while True:
                    data = self.request.recv(65536)
                    if not data:
                        return
                    upstream.sendall(data)
            except OSError:
                return

        request_thread = threading.Thread(target=forward_requests, daemon=True)
        request_thread.start()
        try:
            while True:
                prefix = recv_exact(upstream, 4)
                if prefix is None:
                    return
                frame_length = int.from_bytes(prefix, "big", signed=True)
                if frame_length < 4 or frame_length > 64 * 1024 * 1024:
                    log(self.server, "INVALID_FRAME_LENGTH", connection_id)
                    return
                payload = recv_exact(upstream, frame_length)
                if payload is None:
                    log(self.server, "TRUNCATED_FRAME", connection_id)
                    return
                frame = prefix + payload

                should_delay = False
                with self.server.flag_lock:
                    if self.server.delay_flag.exists():
                        self.server.delay_flag.unlink()
                        should_delay = True
                if should_delay:
                    log(self.server, "DELAY_START", connection_id)
                    time.sleep(self.server.delay_seconds)

                try:
                    self.request.sendall(frame)
                except OSError:
                    log(self.server, "RESPONSE_DROPPED", connection_id)
                else:
                    if not should_delay:
                        log(self.server, "RESPONSE_FORWARDED", connection_id)
                if should_delay:
                    log(self.server, "DELAY_DONE", connection_id)
                    return
        except (ConnectionError, OSError):
            return
        finally:
            try:
                upstream.close()
            except OSError:
                pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, address, upstream, delay_flag, delay_seconds, log_path):
        super().__init__(address, Handler)
        self.upstream = upstream
        self.delay_flag = delay_flag
        self.delay_seconds = delay_seconds
        self.log_path = log_path
        self.flag_lock = threading.Lock()
        self.log_lock = threading.Lock()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upstream", required=True, help="host:port of the disposable Broker")
    parser.add_argument("--delay-ms", type=int, required=True)
    parser.add_argument("--delay-flag", type=Path, required=True)
    parser.add_argument("--ready-file", type=Path, required=True)
    parser.add_argument("--log-file", type=Path, required=True)
    args = parser.parse_args()
    host, port_text = args.upstream.rsplit(":", 1)
    server = Server(
        ("127.0.0.1", 0),
        (host, int(port_text)),
        args.delay_flag,
        args.delay_ms / 1000,
        args.log_file,
    )
    args.log_file.write_text("", encoding="utf-8")
    args.ready_file.write_text(f"{server.server_address[1]}\n", encoding="utf-8")
    print(f"READY\t127.0.0.1\t{server.server_address[1]}", flush=True)
    try:
        server.serve_forever(poll_interval=0.1)
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
