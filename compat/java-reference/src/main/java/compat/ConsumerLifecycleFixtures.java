/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package compat;

import com.alibaba.fastjson2.JSON;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.TopicMessageQueueChangeListener;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.ConsumeMessageService;
import org.apache.rocketmq.client.impl.consumer.DefaultLitePullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPullConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceState;

/** Invoked Java 5.5.1 Consumer factory-start lifecycle oracle. */
public final class ConsumerLifecycleFixtures {
    private ConsumerLifecycleFixtures() { }

    private static MQClientInstance factory(DefaultMQPushConsumerImpl implementation)
        throws ReflectiveOperationException {
        Field field = DefaultMQPushConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        return (MQClientInstance) field.get(implementation);
    }

    private static MQClientInstance factory(DefaultMQPullConsumerImpl implementation)
        throws ReflectiveOperationException {
        Field field = DefaultMQPullConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        return (MQClientInstance) field.get(implementation);
    }

    private static DefaultLitePullConsumerImpl liteImplementation(DefaultLitePullConsumer consumer)
        throws ReflectiveOperationException {
        Field field = DefaultLitePullConsumer.class.getDeclaredField("defaultLitePullConsumerImpl");
        field.setAccessible(true);
        return (DefaultLitePullConsumerImpl) field.get(consumer);
    }

    private static MQClientInstance factory(DefaultLitePullConsumerImpl implementation)
        throws ReflectiveOperationException {
        Field field = DefaultLitePullConsumerImpl.class.getDeclaredField("mQClientFactory");
        field.setAccessible(true);
        return (MQClientInstance) field.get(implementation);
    }

    private static String consumerServiceState(Object implementation)
        throws ReflectiveOperationException {
        Field field = implementation.getClass().getDeclaredField("serviceState");
        field.setAccessible(true);
        return ((ServiceState) field.get(implementation)).name();
    }

    private static String factoryServiceState(MQClientInstance factory)
        throws ReflectiveOperationException {
        Field field = MQClientInstance.class.getDeclaredField("serviceState");
        field.setAccessible(true);
        return ((ServiceState) field.get(factory)).name();
    }

    private static void closeFactory(MQClientInstance factory, String group) throws Exception {
        if (factory == null) return;
        factory.unregisterConsumer(group);
        Field state = MQClientInstance.class.getDeclaredField("serviceState");
        state.setAccessible(true);
        if (factoryServiceState(factory) != ServiceState.SHUTDOWN_ALREADY.name()) {
            state.set(factory, ServiceState.RUNNING);
            factory.shutdown();
        }
        MQClientManager.getInstance().removeClientFactory(factory.getClientId());
    }

    private static void closePushServices(DefaultMQPushConsumerImpl implementation)
        throws Exception {
        ConsumeMessageService consume = implementation.getConsumeMessageService();
        if (consume != null) consume.shutdown(0);
        Field popField = DefaultMQPushConsumerImpl.class
            .getDeclaredField("consumeMessagePopService");
        popField.setAccessible(true);
        ConsumeMessageService pop = (ConsumeMessageService) popField.get(implementation);
        if (pop != null) pop.shutdown(0);
    }

    private static Map<String, Object> failedPushFactoryStart() throws Exception {
        String group = "consumer-start-push-" + System.nanoTime();
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(group);
        consumer.setNamesrvAddr("");
        consumer.setPollNameServerInterval(0);
        consumer.registerMessageListener((MessageListenerConcurrently) (messages, context) ->
            ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultMQPushConsumerImpl implementation = consumer.getDefaultMQPushConsumerImpl();
        MQClientInstance factory = factory(implementation);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", implementation.getServiceState().name());
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closePushServices(implementation);
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected PushConsumer factory-start outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedPullFactoryStart() throws Exception {
        String group = "consumer-start-pull-" + System.nanoTime();
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(group);
        consumer.setNamesrvAddr("");
        consumer.setPollNameServerInterval(0);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultMQPullConsumerImpl implementation = consumer.getDefaultMQPullConsumerImpl();
        MQClientInstance factory = factory(implementation);
        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", implementation.getServiceState().name());
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected PullConsumer factory-start outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullFactoryStart() throws Exception {
        String group = "consumer-start-lite-pull-" + System.nanoTime();
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("");
        consumer.setPollNameServerInterval(0);
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Throwable retryFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? "" : retryFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"IllegalArgumentException".equals(
            startFailure.getClass().getSimpleName())
            || retryFailure == null || !"MQClientException".equals(
                retryFailure.getClass().getSimpleName())
            || !Boolean.TRUE.equals(result.get("consumerRegistered"))
            || !"START_FAILED".equals(result.get("consumerServiceState"))
            || !"START_FAILED".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected LitePullConsumer factory-start outcome: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> failedLitePullListenerRouteLookup() throws Exception {
        String group = "consumer-start-lite-listener-" + System.nanoTime();
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(group);
        consumer.setNamesrvAddr("127.0.0.1:1");
        consumer.setMqClientApiTimeout(100);
        consumer.registerTopicMessageQueueChangeListener("listener-route-failure",
            (TopicMessageQueueChangeListener) (topic, queues) -> { });
        Throwable startFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            startFailure = failure;
        }

        DefaultLitePullConsumerImpl implementation = liteImplementation(consumer);
        MQClientInstance factory = factory(implementation);
        Throwable retryFailure = null;
        try {
            consumer.start();
        } catch (Throwable failure) {
            retryFailure = failure;
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("exception", startFailure == null ? "" : startFailure.getClass().getSimpleName());
        result.put("retryException", retryFailure == null ? "" : retryFailure.getClass().getSimpleName());
        result.put("consumerRegistered", factory != null
            && factory.getConsumerTable().containsKey(group));
        result.put("consumerServiceState", consumerServiceState(implementation));
        result.put("factoryServiceState", factory == null ? "" : factoryServiceState(factory));
        closeFactory(factory, group);

        if (startFailure == null || !"MQClientException".equals(
            startFailure.getClass().getSimpleName())
            || retryFailure == null || !"MQClientException".equals(
                retryFailure.getClass().getSimpleName())
            || !Boolean.FALSE.equals(result.get("consumerRegistered"))
            || !"SHUTDOWN_ALREADY".equals(result.get("consumerServiceState"))
            || !"SHUTDOWN_ALREADY".equals(result.get("factoryServiceState"))) {
            throw new IllegalStateException("Unexpected LitePull listener-route failure: " + result,
                startFailure);
        }
        return result;
    }

    private static Map<String, Object> produce() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("pushFactoryStartFailure", failedPushFactoryStart());
        result.put("pullFactoryStartFailure", failedPullFactoryStart());
        result.put("litePullFactoryStartFailure", failedLitePullFactoryStart());
        result.put("litePullListenerRouteFailure", failedLitePullListenerRouteLookup());
        return result;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1
            && (Boolean.parseBoolean(args[1]) || "--verify".equals(args[1]));
        String json = JSON.toJSONString(produce()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java Consumer lifecycle fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }
}
