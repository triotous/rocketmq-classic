package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQPullConsumer;
import org.apache.rocketmq.client.consumer.MQPushConsumer;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.LitePullConsumer;
import org.apache.rocketmq.client.MQAdmin;
import org.apache.rocketmq.client.impl.consumer.PopProcessQueue;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
import org.apache.rocketmq.client.impl.consumer.RebalanceImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePullImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;

/** Pins the complete public Java consumer-facade and inherited API denominator. */
public final class ConsumerFacadeApiFixtures {
    private ConsumerFacadeApiFixtures() { }

    private static String constructorSignature(Constructor<?> constructor) {
        return constructor.getDeclaringClass().getName() + "("
            + Arrays.stream(constructor.getParameterTypes()).map(Class::getName)
                .collect(Collectors.joining(",")) + ")";
    }

    private static String methodSignature(Method method) {
        return method.getReturnType().getName() + " " + method.getDeclaringClass().getName() + "."
            + method.getName() + "(" + Arrays.stream(method.getParameterTypes()).map(Class::getName)
                .collect(Collectors.joining(",")) + ")";
    }

    private static Map<String, Object> publicApiSurface(Class<?> apiType) {
        List<String> constructors = Arrays.stream(apiType.getDeclaredConstructors())
            .filter(constructor -> Modifier.isPublic(constructor.getModifiers()))
            .map(ConsumerFacadeApiFixtures::constructorSignature).sorted().collect(Collectors.toList());
        List<String> declared = Arrays.stream(apiType.getDeclaredMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .map(ConsumerFacadeApiFixtures::methodSignature).sorted().collect(Collectors.toList());
        List<String> callable = Arrays.stream(apiType.getMethods())
            .filter(method -> Modifier.isPublic(method.getModifiers()))
            .filter(method -> method.getDeclaringClass() != Object.class)
            .map(ConsumerFacadeApiFixtures::methodSignature).distinct().sorted().collect(Collectors.toList());
        Map<String, Object> result = new TreeMap<>();
        result.put("className", apiType.getName());
        result.put("publicConstructors", constructors);
        result.put("declaredPublicMethods", declared);
        result.put("declaredPublicMethodCount", declared.size());
        result.put("publicMethods", callable);
        result.put("publicMethodCount", callable.size());
        return result;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) throw new IllegalArgumentException("expected fixture path");
        Path output = Path.of(args[0]);
        boolean verify = Arrays.asList(args).contains("--verify") || Arrays.asList(args).contains("true");
        Map<String, Object> fixture = new TreeMap<>();
        Map<String, Object> provenance = new TreeMap<>();
        provenance.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        provenance.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        provenance.put("serverBaselineContacted", false);
        fixture.put("provenance", provenance);
        Map<String, Object> apis = new TreeMap<>();
        apis.put("DefaultMQPushConsumer", publicApiSurface(DefaultMQPushConsumer.class));
        apis.put("MQPushConsumer", publicApiSurface(MQPushConsumer.class));
        apis.put("DefaultMQPullConsumer", publicApiSurface(DefaultMQPullConsumer.class));
        apis.put("MQPullConsumer", publicApiSurface(MQPullConsumer.class));
        apis.put("DefaultLitePullConsumer", publicApiSurface(DefaultLitePullConsumer.class));
        apis.put("LitePullConsumer", publicApiSurface(LitePullConsumer.class));
        apis.put("MQAdmin", publicApiSurface(MQAdmin.class));
        fixture.put("apis", apis);
        Map<String, Object> implementationApis = new TreeMap<>();
        implementationApis.put("RebalanceImpl", publicApiSurface(RebalanceImpl.class));
        implementationApis.put("RebalancePushImpl", publicApiSurface(RebalancePushImpl.class));
        implementationApis.put("RebalancePullImpl", publicApiSurface(RebalancePullImpl.class));
        implementationApis.put("ProcessQueue", publicApiSurface(ProcessQueue.class));
        implementationApis.put("PopProcessQueue", publicApiSurface(PopProcessQueue.class));
        fixture.put("implementationApis", implementationApis);

        String json = JSON.toJSONString(fixture) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("fixture drift: " + output);
            }
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
        }
    }
}
