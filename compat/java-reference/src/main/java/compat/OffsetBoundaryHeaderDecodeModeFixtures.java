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
import com.alibaba.fastjson2.JSONWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.CommandCustomHeader;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.remoting.protocol.header.SearchOffsetResponseHeader;

/** Invokes Java 5.5.1 reflection decoding for offset-boundary response headers. */
public final class OffsetBoundaryHeaderDecodeModeFixtures {
    private OffsetBoundaryHeaderDecodeModeFixtures() { }

    public static void main(String[] args) throws Exception {
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");

        Map<String, Object> cases = new TreeMap<>();
        cases.put("search-missing", decodeModes("search", "offset", new TreeMap<>()));
        cases.put("search-invalid", decodeModes("search", "offset", invalid("offset")));
        cases.put("search-valid", decodeModes("search", "offset", valid("offset", "9")));
        cases.put("max-missing", decodeModes("max", "offset", new TreeMap<>()));
        cases.put("max-invalid", decodeModes("max", "offset", invalid("offset")));
        cases.put("max-valid", decodeModes("max", "offset", valid("offset", "10")));
        cases.put("min-missing", decodeModes("min", "offset", new TreeMap<>()));
        cases.put("min-invalid", decodeModes("min", "offset", invalid("offset")));
        cases.put("min-valid", decodeModes("min", "offset", valid("offset", "1")));
        cases.put("earliest-missing", decodeModes("earliest", "timestamp", new TreeMap<>()));
        cases.put("earliest-invalid", decodeModes("earliest", "timestamp", invalid("timestamp")));
        cases.put("earliest-valid", decodeModes("earliest", "timestamp", valid("timestamp", "-1")));
        fixture.put("cases", cases);

        String json = JSON.toJSONString(fixture, JSONWriter.Feature.WriteMapNullValue) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) {
                throw new IllegalStateException("fixture drift: " + file);
            }
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Offset-boundary response-header decode fixtures verified");
    }

    private static Map<String, String> invalid(String fieldName) {
        return valid(fieldName, "not-a-long");
    }

    private static Map<String, String> valid(String fieldName, String value) {
        Map<String, String> fields = new TreeMap<>();
        fields.put(fieldName, value);
        return fields;
    }

    private static Map<String, Object> decodeModes(String headerType, String fieldName,
        Map<String, String> fields) {
        Map<String, Object> result = new TreeMap<>();
        result.put("fields", new TreeMap<>(fields));
        result.put("headerType", headerType);
        result.put("fieldName", fieldName);
        result.put("fast", decode(headerType, fieldName, fields, true));
        result.put("reflection", decode(headerType, fieldName, fields, false));
        return result;
    }

    private static Map<String, Object> decode(String headerType, String fieldName,
        Map<String, String> fields, boolean useFastEncode) {
        Map<String, Object> result = new TreeMap<>();
        RemotingCommand command = RemotingCommand.createRequestCommand(29, null);
        command.setExtFields(new HashMap<>(fields));
        try {
            Class<? extends CommandCustomHeader> headerClass;
            switch (headerType) {
                case "search":
                    headerClass = SearchOffsetResponseHeader.class;
                    break;
                case "max":
                    headerClass = GetMaxOffsetResponseHeader.class;
                    break;
                case "min":
                    headerClass = GetMinOffsetResponseHeader.class;
                    break;
                case "earliest":
                    headerClass = GetEarliestMsgStoretimeResponseHeader.class;
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + headerType);
            }
            CommandCustomHeader header = command.decodeCommandCustomHeaderDirectly(
                headerClass, useFastEncode);
            result.put("outcome", "OK");
            result.put("value", readField(header, fieldName));
            recordOwnerUnboxResult(result, headerType, header);

            RemotingCommand encoded = RemotingCommand.createRequestCommand(29, null);
            encoded.setExtFields(new HashMap<>());
            encoded.writeCustomHeader(header);
            encoded.makeCustomHeaderToNet();
            result.put("encodedFields", new TreeMap<>(encoded.getExtFields()));
        } catch (Throwable error) {
            result.put("outcome", error.getClass().getSimpleName());
        }
        return result;
    }

    private static void recordOwnerUnboxResult(Map<String, Object> result, String headerType,
        CommandCustomHeader header) {
        try {
            long value;
            switch (headerType) {
                case "search":
                    value = ((SearchOffsetResponseHeader) header).getOffset();
                    break;
                case "max":
                    value = ((GetMaxOffsetResponseHeader) header).getOffset();
                    break;
                case "min":
                    value = ((GetMinOffsetResponseHeader) header).getOffset();
                    break;
                case "earliest":
                    value = ((GetEarliestMsgStoretimeResponseHeader) header).getTimestamp();
                    break;
                default:
                    throw new IllegalArgumentException("unknown header type: " + headerType);
            }
            result.put("ownerOutcome", "OK");
            result.put("ownerValue", value);
        } catch (Throwable error) {
            result.put("ownerOutcome", error.getClass().getSimpleName());
        }
    }

    private static Object readField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }
}
