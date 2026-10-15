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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;

/** Invoked Java 5.5.1 identity-equality oracle for response headers. */
public final class HeaderResponseIdentityFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";
    private static final Map<String, String> RUST_TO_JAVA_CLASSES = Map.ofEntries(
        Map.entry("QueryConsumerOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.QueryConsumerOffsetResponseHeader"),
        Map.entry("SearchOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader"),
        Map.entry("GetMaxOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader"),
        Map.entry("GetMinOffsetResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader"),
        Map.entry("GetEarliestMsgStoretimeResponseHeader", "org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader"),
        Map.entry("PullMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PullMessageResponseHeader"),
        Map.entry("QueryMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.QueryMessageResponseHeader"),
        Map.entry("RecallMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.RecallMessageResponseHeader"),
        Map.entry("SendMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.SendMessageResponseHeader"),
        Map.entry("UnregisterClientResponseHeader", "org.apache.rocketmq.remoting.protocol.header.UnregisterClientResponseHeader"),
        Map.entry("EndTransactionResponseHeader", "org.apache.rocketmq.remoting.protocol.header.EndTransactionResponseHeader"),
        Map.entry("CheckTransactionStateResponseHeader", "org.apache.rocketmq.remoting.protocol.header.CheckTransactionStateResponseHeader"),
        Map.entry("NotificationResponseHeader", "org.apache.rocketmq.remoting.protocol.header.NotificationResponseHeader"),
        Map.entry("PopMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PopMessageResponseHeader"),
        Map.entry("PopLiteMessageResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PopLiteMessageResponseHeader"),
        Map.entry("PollingInfoResponseHeader", "org.apache.rocketmq.remoting.protocol.header.PollingInfoResponseHeader"));

    private HeaderResponseIdentityFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            if (!Files.exists(path) || !json.equals(Files.readString(path))) {
                throw new IllegalStateException("Java header response identity fixture drift: " + path);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json, StandardCharsets.UTF_8);
        }
    }

    private static Map<String, Object> render() throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("serverContacted", false);
        Map<String, Object> identityCases = new TreeMap<>();
        for (Map.Entry<String, String> entry : RUST_TO_JAVA_CLASSES.entrySet()) {
            Class<?> type = Class.forName(entry.getValue());
            Object first = type.getDeclaredConstructor().newInstance();
            Object second = type.getDeclaredConstructor().newInstance();
            identityCases.put(entry.getKey(), identityCase(type, first, second));
        }
        result.put("identityCases", identityCases);
        return result;
    }

    private static Map<String, Object> identityCase(Class<?> type, Object first, Object second)
        throws Exception {
        Map<String, Object> result = new TreeMap<>();
        result.put("javaClass", type.getName());
        result.put("equalsDeclaredBy", type.getMethod("equals", Object.class)
            .getDeclaringClass().getName());
        result.put("hashCodeDeclaredBy", type.getMethod("hashCode")
            .getDeclaringClass().getName());
        result.put("sameObjectEqualsItself", first.equals(first));
        result.put("distinctDefaultObjectsEquals", first.equals(second));
        return result;
    }
}
