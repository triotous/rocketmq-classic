package compat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
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
        verifyCallbackExecutorSetting();
        verifyScanAvailableNameServerSetting();
        verifyClientWorkerThreadsSetting();
        verifyConnectTimeoutSetting();
        verifyProxyConnectTimeoutSetting();
        System.out.println("Remoting live config mutation verified");
    }

    private static void verifyCallbackExecutorSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        NettyRemotingClient client = new NettyRemotingClient(config);
        ExecutorService custom = Executors.newSingleThreadExecutor();
        try {
            client.start();
            client.setCallbackExecutor(custom);
            config.setDisableCallbackExecutor(true);
            if (client.getCallbackExecutor() != null) {
                throw new IllegalStateException("disabled callback executor remained externally visible");
            }
            client.setCallbackExecutor(custom);
            if (client.getCallbackExecutor() != null) {
                throw new IllegalStateException("setting a custom executor bypassed the live disable flag");
            }
            config.setDisableCallbackExecutor(false);
            if (client.getCallbackExecutor() != custom) {
                throw new IllegalStateException("re-enabled callback executor did not retain the custom executor");
            }
        } finally {
            client.shutdown();
            custom.shutdown();
        }
    }

    private static void verifyScanAvailableNameServerSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setScanAvailableNameSrv(false);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            server.setSoTimeout(3000);
            client.updateNameServerAddressList(
                java.util.Collections.singletonList("127.0.0.1:" + server.getLocalPort()));
            config.setScanAvailableNameSrv(true);
            client.start();
            try (Socket accepted = server.accept()) {
                if (!accepted.isConnected()) {
                    throw new IllegalStateException("start-time scan did not establish its probe connection");
                }
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyClientWorkerThreadsSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientWorkerThreads(1);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            config.setClientWorkerThreads(3);
            client.start();
            Field field = NettyRemotingClient.class.getDeclaredField("defaultEventExecutorGroup");
            field.setAccessible(true);
            Object executorGroup = field.get(client);
            if (!(executorGroup instanceof Iterable<?>)) {
                throw new IllegalStateException("client worker executor group was not initialized");
            }
            int count = 0;
            for (Object ignored : (Iterable<?>) executorGroup) {
                count++;
            }
            if (count != 3) {
                throw new IllegalStateException("start did not use the live worker count: " + count);
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyConnectTimeoutSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setConnectTimeoutMillis(1);
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            config.setConnectTimeoutMillis(2345);
            client.start();
            Field field = NettyRemotingClient.class.getDeclaredField("bootstrap");
            field.setAccessible(true);
            Bootstrap bootstrap = (Bootstrap) field.get(client);
            Object configured = bootstrap.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(2345).equals(configured)) {
                throw new IllegalStateException("start did not use the live connect timeout: " + configured);
            }
        } finally {
            client.shutdown();
        }
    }

    private static void verifyProxyConnectTimeoutSetting() throws Exception {
        NettyClientConfig config = new NettyClientConfig();
        config.setConnectTimeoutMillis(1);
        config.setSocksProxyConfig(
            "{\"127.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"},"
                + "\"10.0.0.0/8\":{\"addr\":\"127.0.0.1:1080\"}}");
        NettyRemotingClient client = new NettyRemotingClient(config);
        try {
            client.start();
            config.setConnectTimeoutMillis(2345);

            Field directField = NettyRemotingClient.class.getDeclaredField("bootstrap");
            directField.setAccessible(true);
            Bootstrap direct = (Bootstrap) directField.get(client);
            Object directTimeout = direct.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(1).equals(directTimeout)) {
                throw new IllegalStateException("direct bootstrap changed after start: " + directTimeout);
            }

            java.lang.reflect.Method fetchBootstrap = NettyRemotingClient.class
                .getDeclaredMethod("fetchBootstrap", String.class);
            fetchBootstrap.setAccessible(true);
            Bootstrap firstProxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            Object firstTimeout = firstProxy.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(2345).equals(firstTimeout)) {
                throw new IllegalStateException("first proxy bootstrap missed live timeout: " + firstTimeout);
            }

            config.setConnectTimeoutMillis(6789);
            Bootstrap reusedProxy = (Bootstrap) fetchBootstrap.invoke(client, "127.0.0.1:10911");
            if (reusedProxy != firstProxy) {
                throw new IllegalStateException("proxy bootstrap was not reused for its CIDR");
            }
            Object cachedTimeout = reusedProxy.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(2345).equals(cachedTimeout)) {
                throw new IllegalStateException("cached proxy bootstrap changed timeout: " + cachedTimeout);
            }

            Bootstrap secondProxy = (Bootstrap) fetchBootstrap.invoke(client, "10.0.0.1:10911");
            Object secondTimeout = secondProxy.config().options().get(ChannelOption.CONNECT_TIMEOUT_MILLIS);
            if (!Integer.valueOf(6789).equals(secondTimeout)) {
                throw new IllegalStateException("new proxy bootstrap missed latest timeout: " + secondTimeout);
            }
        } finally {
            client.shutdown();
        }
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

