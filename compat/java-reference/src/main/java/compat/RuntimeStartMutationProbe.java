package compat;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;

/**
 * Fork-only probe for the post-construction NameServer startup branch. The
 * child halts after writing its observation because the pinned runtime starts
 * process-lifetime worker threads that can outlive shutdown.
 */
public final class RuntimeStartMutationProbe {
    private RuntimeStartMutationProbe() { }

    private static final class StartProbeApi extends MQClientAPIImpl {
        final AtomicInteger discoveryCalls = new AtomicInteger();

        StartProbeApi(ClientConfig config) {
            super(new NettyClientConfig(), null, null, config);
        }

        @Override
        public String fetchNameServerAddr() {
            discoveryCalls.incrementAndGet();
            return null;
        }

        @Override
        public void start() { }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("expected fixture output path");
        Map<String, Object> item = new TreeMap<>();
        ClientConfig config = new ClientConfig();
        config.setClientIP("192.0.2.6");
        MQClientInstance runtime = new MQClientInstance(config, 73, config.buildMQClientId());
        StartProbeApi api = new StartProbeApi(config);
        Field apiField = MQClientInstance.class.getDeclaredField("mQClientAPIImpl");
        apiField.setAccessible(true);
        apiField.set(runtime, api);
        List<String> activeAddressesBefore = api.getNameServerAddressList();
        item.put("activeAddressesBeforeMutation", activeAddressesBefore);
        config.setNamesrvAddr("late-after-construction:9876");
        item.put("namesrvAddrAtStart", config.getNamesrvAddr());
        item.put("activeAddressesBeforeStart", api.getNameServerAddressList());

        Field executorField = MQClientInstance.class.getDeclaredField("scheduledExecutorService");
        executorField.setAccessible(true);
        ScheduledExecutorService original = (ScheduledExecutorService) executorField.get(runtime);
        original.shutdownNow();
        AtomicInteger scheduledCalls = new AtomicInteger();
        ScheduledExecutorService recorder = (ScheduledExecutorService) java.lang.reflect.Proxy
            .newProxyInstance(RuntimeStartMutationProbe.class.getClassLoader(),
                new Class<?>[] {ScheduledExecutorService.class}, (proxy, method, values) -> {
                    if ("scheduleAtFixedRate".equals(method.getName())) {
                        scheduledCalls.incrementAndGet();
                        return null;
                    }
                    if ("shutdownNow".equals(method.getName())) return new ArrayList<>();
                    if ("isShutdown".equals(method.getName()) || "isTerminated".equals(method.getName())) return false;
                    if ("awaitTermination".equals(method.getName())) return true;
                    if ("equals".equals(method.getName())) return proxy == values[0];
                    if ("hashCode".equals(method.getName())) return System.identityHashCode(proxy);
                    if ("toString".equals(method.getName())) return "start-probe scheduler";
                    return null;
                });
        executorField.set(runtime, recorder);

        try {
            runtime.start();
            item.put("startOutcome", "RUNNING");
        } catch (Throwable error) {
            item.put("startOutcome", error.getClass().getSimpleName());
        }
        item.put("discoveryCalls", api.discoveryCalls.get());
        item.put("scheduledTaskCount", scheduledCalls.get());
        item.put("activeAddressesAfterStart", api.getNameServerAddressList());
        item.put("serverBaselineContacted", false);
        try {
            runtime.shutdown();
            item.put("shutdownOutcome", "OK");
        } catch (Throwable error) {
            item.put("shutdownOutcome", error.getClass().getSimpleName());
        }

        String json = com.alibaba.fastjson2.JSON.toJSONString(item,
            com.alibaba.fastjson2.JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        boolean verify = Boolean.parseBoolean(System.getProperty("start.fixture.verify", "true"));
        if (verify) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        if (!"RUNNING".equals(item.get("startOutcome")) || !"OK".equals(item.get("shutdownOutcome"))) {
            throw new IllegalStateException("runtime start/shutdown probe failed: " + item);
        }
        System.out.println("Runtime start mutation probe verified");
        System.out.flush();
        Runtime.getRuntime().halt(0);
    }
}
