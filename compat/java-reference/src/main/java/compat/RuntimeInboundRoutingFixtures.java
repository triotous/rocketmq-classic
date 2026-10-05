package compat;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;

/** Routing metadata only; payload decoding/transaction handling belongs to later phases. */
public final class RuntimeInboundRoutingFixtures {
    private RuntimeInboundRoutingFixtures() { }
    public static void main(String[] args) throws Exception {
        List<Object> cases = new ArrayList<>();
        for (int version : new int[] {1,2}) for (boolean bornV6 : new boolean[] {false,true}) for (boolean storeV6 : new boolean[] {false,true}) {
            MessageExt message = new MessageExt(); message.setTopic("topic"); message.setBody(new byte[] {0,-1,42});
            message.setBornHost(new InetSocketAddress(bornV6 ? "::1" : "127.0.0.1",100));
            message.setStoreHost(new InetSocketAddress(storeV6 ? "::1" : "127.0.0.1",200));
            message.setSysFlag((bornV6 ? MessageSysFlag.BORNHOST_V6_FLAG : 0) | (storeV6 ? MessageSysFlag.STOREHOSTADDRESS_V6_FLAG : 0));
            String group = bornV6 ? "producer-b" : "producer-a";
            org.apache.rocketmq.common.message.MessageAccessor.setProperties(message,new TreeMap<>(Map.of("PGROUP",group,"UNIQ_KEY","id")));
            byte[] body = MessageDecoder.encode(message,false);
            if (version == 2) {
                // encode emits V1. Adapt only the version/topic-width fields,
                // then invoke actual Java decodeProperties as the V2 oracle.
                int topicPosition = 88 + (bornV6 ? 12 : 0) + (storeV6 ? 12 : 0) + message.getBody().length;
                ByteBuffer converted = ByteBuffer.allocate(body.length+1);
                converted.put(body,0,topicPosition).putShort((short)5).put(body,topicPosition+1,body.length-topicPosition-1);
                converted.putInt(0,body.length+1); converted.putInt(4,MessageDecoder.MESSAGE_MAGIC_CODE_V2); body = converted.array();
            }
            Map<String,Object> item = new TreeMap<>();
            item.put("case","v"+version+"-born"+(bornV6 ? 6 : 4)+"-store"+(storeV6 ? 6 : 4));
            item.put("group",group); item.put("body",Base64.getEncoder().encodeToString(body));
            item.put("properties",MessageDecoder.decodeProperties(ByteBuffer.wrap(body))); cases.add(item);
        }
        Map<String,Object> fixture = new TreeMap<>(); fixture.put("client","org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit","efa1be59d6fc747ad220f036c86030f05f0419f9"); fixture.put("entrypoint","MessageDecoder.encode/decodeProperties; MessageVersion");
        fixture.put("serverBaselineContacted",false); fixture.put("cases",cases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n"; Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else { Files.createDirectories(file.getParent()); Files.writeString(file,json); }
        System.out.println("Runtime inbound routing invoked cases verified: " + cases.size());
    }
}
