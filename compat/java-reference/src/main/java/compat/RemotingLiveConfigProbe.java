package compat;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;

/** Verifies that a live NettyClientConfig timeout setting is read by an existing client. */
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
        System.out.println("Remoting live config mutation verified");
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

