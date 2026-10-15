package compat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

/** Verifies that live NettyClientConfig settings are read by an existing client. */
public final class RemotingLiveConfigProbe {
    private RemotingLiveConfigProbe() {
    }

    public static void main(String[] args) throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientCloseSocketIfTimeout(true);
        NettyRemotingClient client = new NettyRemotingClient(config);
        CompletableFuture<Boolean> peerStayedOpen = new CompletableFuture<>();
        Thread peer;
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(3000);
            peer = new Thread(() -> observePeer(server, peerStayedOpen), "live-config-peer");
            peer.start();
            client.start();
            config.setClientCloseSocketIfTimeout(false);
            try {
                client.invokeSync(
                    "127.0.0.1:" + server.getLocalPort(),
                    RemotingCommand.createRequestCommand(0, null),
                    2000);
                throw new IllegalStateException("invokeSync unexpectedly received a response");
            } catch (RemotingTimeoutException expected) {
                // The Java client must consult the retained config at timeout handling time.
            }
            if (!peerStayedOpen.get(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("post-construction false setting did not keep the socket open");
            }
        } finally {
            client.shutdown();
        }
        peer.join(3000);
        if (peer.isAlive()) {
            throw new IllegalStateException("scripted peer did not terminate after client shutdown");
        }
        verifyGoAwayReconnectSetting();
        System.out.println("Remoting live config mutation verified");
    }

    private static void verifyGoAwayReconnectSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setEnableReconnectForGoAway(false);
        NettyRemotingClient client = new NettyRemotingClient(config);
        CompletableFuture<Boolean> retryObserved = new CompletableFuture<>();
        Thread peer;
        try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(5000);
            peer = new Thread(() -> observeGoAwayRetry(server, retryObserved), "live-config-go-away-peer");
            peer.start();
            client.start();
            config.setEnableReconnectForGoAway(true);
            RemotingCommand response = client.invokeSync(
                "127.0.0.1:" + server.getLocalPort(),
                RemotingCommand.createRequestCommand(0, null),
                3000);
            if (response == null || response.getCode() != ResponseCode.SUCCESS) {
                throw new IllegalStateException("post-start reconnect setting did not retry after GO_AWAY");
            }
            if (!retryObserved.get(3, TimeUnit.SECONDS)) {
                throw new IllegalStateException("replacement connection did not carry the retry");
            }
        } finally {
            client.shutdown();
        }
        peer.join(3000);
        if (peer.isAlive()) {
            throw new IllegalStateException("GO_AWAY scripted peer did not terminate after client shutdown");
        }
    }

    private static void observeGoAwayRetry(ServerSocket server, CompletableFuture<Boolean> retryObserved) {
        try (Socket first = server.accept()) {
            RemotingCommand request = readCommand(first);
            RemotingCommand goAway = RemotingCommand.createResponseCommand(ResponseCode.GO_AWAY, "retry");
            goAway.setSerializeTypeCurrentRPC(request.getSerializeTypeCurrentRPC());
            goAway.setOpaque(request.getOpaque());
            writeCommand(first.getOutputStream(), goAway);
        } catch (IOException error) {
            retryObserved.completeExceptionally(error);
            return;
        }

        try (Socket second = server.accept()) {
            RemotingCommand retry = readCommand(second);
            RemotingCommand success = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
            success.setSerializeTypeCurrentRPC(retry.getSerializeTypeCurrentRPC());
            success.setOpaque(retry.getOpaque());
            writeCommand(second.getOutputStream(), success);
            retryObserved.complete(true);
        } catch (IOException error) {
            retryObserved.completeExceptionally(error);
        }
    }

    private static RemotingCommand readCommand(Socket socket) throws IOException {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        int frameLength = input.readInt();
        if (frameLength <= 0 || frameLength > 16 * 1024 * 1024) {
            throw new IOException("invalid Remoting frame length: " + frameLength);
        }
        byte[] frame = new byte[frameLength];
        input.readFully(frame);
        try {
            return RemotingCommand.decode(ByteBuffer.wrap(frame));
        } catch (Exception error) {
            throw new IOException("failed to decode scripted Remoting frame", error);
        }
    }

    private static void writeCommand(OutputStream output, RemotingCommand command) throws IOException {
        ByteBuffer encoded = command.encode();
        byte[] frame = new byte[encoded.remaining()];
        encoded.get(frame);
        output.write(frame);
        output.flush();
    }

    private static void observePeer(ServerSocket server, CompletableFuture<Boolean> peerStayedOpen) {
        try (Socket socket = server.accept()) {
            DataInputStream input = new DataInputStream(socket.getInputStream());
            int frameLength = input.readInt();
            byte[] frame = new byte[frameLength];
            input.readFully(frame);
            socket.setSoTimeout(2500);
            try {
                peerStayedOpen.complete(input.read() >= 0);
            } catch (SocketTimeoutException expected) {
                peerStayedOpen.complete(true);
            }
        } catch (IOException error) {
            peerStayedOpen.completeExceptionally(error);
        }
    }
}

