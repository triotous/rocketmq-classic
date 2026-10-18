/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.client.MqClientAdmin;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.admin.MqClientAdminImpl;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.remoting.protocol.RequestCode;
import org.apache.rocketmq.remoting.protocol.ResponseCode;
import org.apache.rocketmq.remoting.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.remoting.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.remoting.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteTopicFromNamesrvRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.namesrv.DeleteKVConfigRequestHeader;
import org.apache.rocketmq.remoting.protocol.header.QueryConsumeTimeSpanRequestHeader;

/** Invokes the pinned Java MqClientAdmin asynchronous administrative RPCs. */
public final class MqClientAdminFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final String BROKER_ADDRESS = "127.0.0.1:10911";
    private static final long TIMEOUT_MILLIS = 1234L;

    private MqClientAdminFixtures() { }

    private static final class Capture {
        String address;
        long timeoutMillis;
        RemotingCommand request;
        RemotingCommand response;

        RemotingClient proxy() {
            return (RemotingClient) java.lang.reflect.Proxy.newProxyInstance(
                RemotingClient.class.getClassLoader(), new Class<?>[] {RemotingClient.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("invoke")) {
                        address = (String) args[0];
                        request = (RemotingCommand) args[1];
                        timeoutMillis = ((Number) args[2]).longValue();
                        request.makeCustomHeaderToNet();
                        return CompletableFuture.completedFuture(response);
                    }
                    return method.getReturnType() == boolean.class ? false : null;
                });
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            throw new IllegalArgumentException(
                "usage: MqClientAdminFixtures <fixture.json> [--verify]");
        }
        Path output = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(generate()) + "\n";
        if (verify) {
            if (!Files.exists(output) || !json.equals(Files.readString(output))) {
                throw new IllegalStateException("Java MqClientAdmin fixture drift: " + output);
            }
            System.out.println("verified Java 5.5.1 MqClientAdmin fixture at " + output);
        } else {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json);
            System.out.println("wrote Java 5.5.1 MqClientAdmin fixture to " + output);
        }
    }

    private static Map<String, Object> generate() throws Exception {
        Method method = MqClientAdmin.class.getMethod("queryConsumeTimeSpan",
            String.class, QueryConsumeTimeSpanRequestHeader.class, long.class);
        QueryConsumeTimeSpanRequestHeader header = new QueryConsumeTimeSpanRequestHeader();
        header.setTopic("orders");
        header.setGroup("workers");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        QueryConsumeTimeSpanBody body = new QueryConsumeTimeSpanBody();
        QueueTimeSpan span = new QueueTimeSpan();
        span.setMessageQueue(new MessageQueue("orders", "broker-a", 1));
        span.setMinTimeStamp(1_700_000_000_000L);
        span.setMaxTimeStamp(1_700_000_100_000L);
        span.setConsumeTimeStamp(1_700_000_050_000L);
        span.setDelayTime(50_000L);
        body.setConsumeTimeSpanSet(List.of(span));
        byte[] bodyBytes = RemotingSerializable.encode(body);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        successCapture.response.setBody(bodyBytes);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        List<QueueTimeSpan> spans = successAdmin
            .queryConsumeTimeSpan(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> errorOutcome = new LinkedHashMap<>();
        try {
            errorAdmin.queryConsumeTimeSpan(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin Broker error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            errorOutcome.put("exceptionClass", clientException.getClass().getName());
            errorOutcome.put("responseCode", clientException.getResponseCode());
            errorOutcome.put("message", clientException.getMessage());
        }

        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("responseBody", new String(bodyBytes, StandardCharsets.UTF_8));
        List<Map<String, Object>> rows = new ArrayList<>();
        for (QueueTimeSpan value : spans) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("topic", value.getMessageQueue().getTopic());
            row.put("brokerName", value.getMessageQueue().getBrokerName());
            row.put("queueId", value.getMessageQueue().getQueueId());
            row.put("minTimeStamp", value.getMinTimeStamp());
            row.put("maxTimeStamp", value.getMaxTimeStamp());
            row.put("consumeTimeStamp", value.getConsumeTimeStamp());
            row.put("delayTime", value.getDelayTime());
            rows.add(row);
        }
        success.put("result", rows);

        Map<String, Object> error = new LinkedHashMap<>();
        error.put("address", errorCapture.address);
        error.put("timeoutMillis", errorCapture.timeoutMillis);
        error.put("requestCode", errorCapture.request.getCode());
        error.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        error.put("responseCode", errorCapture.response.getCode());
        error.put("responseRemark", errorCapture.response.getRemark());
        error.put("outcome", errorOutcome);

        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("artifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", SOURCE_COMMIT);
        fixture.put("method", method.toGenericString());
        fixture.put("success", success);
        fixture.put("brokerError", error);
        fixture.put("updateOrCreateTopic", updateOrCreateTopic());
        fixture.put("deleteTopicInBroker", deleteTopicInBroker());
        fixture.put("deleteTopicInNameserver", deleteTopicInNameserver());
        fixture.put("deleteKvConfig", deleteKvConfig());
        return fixture;
    }

    private static Map<String, Object> deleteTopicInBroker() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteTopicInBroker",
            String.class, DeleteTopicRequestHeader.class, long.class);
        DeleteTopicRequestHeader header = new DeleteTopicRequestHeader();
        header.setTopic("orders");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);
        header.setLo(true);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteTopicInBroker(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteTopicInBroker(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin delete topic error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> deleteTopicInNameserver() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteTopicInNameserver",
            String.class, DeleteTopicFromNamesrvRequestHeader.class, long.class);
        DeleteTopicFromNamesrvRequestHeader header = new DeleteTopicFromNamesrvRequestHeader();
        header.setTopic("orders");
        header.setClusterName("cluster-a");
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);
        header.setLo(true);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteTopicInNameserver(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.TOPIC_NOT_EXIST, "topic not exist");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteTopicInNameserver(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin nameserver delete error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> deleteKvConfig() throws Exception {
        Method method = MqClientAdmin.class.getMethod("deleteKvConfig",
            String.class, DeleteKVConfigRequestHeader.class, long.class);
        DeleteKVConfigRequestHeader header = new DeleteKVConfigRequestHeader();
        header.setNamespace("config-namespace");
        header.setKey("config-key");

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.deleteKvConfig(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "kv config delete failed");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.deleteKvConfig(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin delete KV config error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
            outcome.put("message", clientException.getMessage());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }

    private static Map<String, Object> updateOrCreateTopic() throws Exception {
        Method method = MqClientAdmin.class.getMethod("updateOrCreateTopic",
            String.class, CreateTopicRequestHeader.class, long.class);
        CreateTopicRequestHeader header = new CreateTopicRequestHeader();
        header.setTopic("orders");
        header.setDefaultTopic("TBW102");
        header.setReadQueueNums(4);
        header.setWriteQueueNums(4);
        header.setPerm(6);
        header.setTopicFilterType("SINGLE_TAG");
        header.setTopicSysFlag(3);
        header.setOrder(false);
        header.setAttributes("+unit=true");
        header.setForce(false);
        header.setNamespace("tenant");
        header.setNamespaced(true);
        header.setBrokerName("broker-a");
        header.setOneway(false);

        Capture successCapture = new Capture();
        successCapture.response = RemotingCommand.createResponseCommand(ResponseCode.SUCCESS, null);
        MqClientAdminImpl successAdmin = new MqClientAdminImpl(successCapture.proxy());
        successAdmin.updateOrCreateTopic(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
        Map<String, Object> success = new LinkedHashMap<>();
        success.put("address", successCapture.address);
        success.put("timeoutMillis", successCapture.timeoutMillis);
        success.put("requestCode", successCapture.request.getCode());
        success.put("requestExtFields", new TreeMap<>(successCapture.request.getExtFields()));
        success.put("requestBody", successCapture.request.getBody());
        success.put("responseCode", successCapture.response.getCode());
        success.put("completed", true);

        Capture errorCapture = new Capture();
        errorCapture.response = RemotingCommand.createResponseCommand(
            ResponseCode.SYSTEM_ERROR, "topic update failed");
        MqClientAdminImpl errorAdmin = new MqClientAdminImpl(errorCapture.proxy());
        Map<String, Object> outcome = new LinkedHashMap<>();
        try {
            errorAdmin.updateOrCreateTopic(BROKER_ADDRESS, header, TIMEOUT_MILLIS).get();
            throw new IllegalStateException("expected MqClientAdmin topic update error");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof MQClientException)) {
                throw exception;
            }
            MQClientException clientException = (MQClientException) exception.getCause();
            outcome.put("exceptionClass", clientException.getClass().getName());
            outcome.put("responseCode", clientException.getResponseCode());
        }
        Map<String, Object> brokerError = new LinkedHashMap<>();
        brokerError.put("address", errorCapture.address);
        brokerError.put("timeoutMillis", errorCapture.timeoutMillis);
        brokerError.put("requestCode", errorCapture.request.getCode());
        brokerError.put("requestExtFields", new TreeMap<>(errorCapture.request.getExtFields()));
        brokerError.put("responseCode", errorCapture.response.getCode());
        brokerError.put("responseRemark", errorCapture.response.getRemark());
        brokerError.put("outcome", outcome);

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("method", method.toGenericString());
        result.put("success", success);
        result.put("brokerError", brokerError);
        return result;
    }
}
