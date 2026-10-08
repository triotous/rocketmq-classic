package compat;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.utils.NetworkUtil;

/** Deterministic address helpers only: no DNS, interface enumeration or Broker. */
public final class RuntimeAddressFixtures {
    private RuntimeAddressFixtures() { }

    public static void main(String[] args) throws Exception {
        List<Object> addresses = new ArrayList<>();
        String[] literals = {"0.0.0.0", "10.0.0.1", "11.0.0.1", "127.0.0.2", "169.254.1.1",
            "172.15.0.1", "172.16.0.1", "172.31.255.255", "172.32.0.1", "192.168.1.1",
            "192.169.1.1", "192.0.2.1", "255.255.255.255", "::", "::1", "fe7f::1",
            "fe80::1", "febf::1", "fec0::1", "feff::1", "ff00::1", "fc00::1", "fdff::1",
            "2001:db8::ABCD", "::ffff:192.0.2.1"};
        for (String literal : literals) {
            // Numeric literals avoid name lookup. Keep mapped IPv6 as 16 bytes:
            // InetAddress.getByName otherwise converts it to Inet4Address.
            InetAddress ip = literal.equals("::ffff:192.0.2.1")
                ? Inet6Address.getByAddress(null, new byte[] {0,0,0,0,0,0,0,0,0,0,-1,-1,-64,0,2,1}, -1)
                : InetAddress.getByName(literal);
            add(addresses, literal, 0, ip);
        }
        add(addresses, "fe80::1", 7, Inet6Address.getByAddress(null,
            InetAddress.getByName("fe80::1").getAddress(), 7));
        List<Object> clientAddressCases = new ArrayList<>();
        AddressProbe addressProbe = new AddressProbe();
        try {
            for (String address : new String[] {"127.0.0.1:10911", "broker.example:10911",
                    "2001:db8::1:10911", "::1:10911", "[2001:db8::1]:10911"}) {
                String[] hostAndPort = addressProbe.splitAddress(address);
                if (hostAndPort.length != 2) {
                    throw new IllegalStateException("Java client did not split address: " + address);
                }
                Map<String, Object> item = new TreeMap<>();
                item.put("address", address);
                item.put("host", hostAndPort[0]);
                item.put("port", hostAndPort[1]);
                clientAddressCases.add(item);
            }
        } finally {
            addressProbe.shutdown();
        }
        Map<String, Object> fixture = new TreeMap<>();
        fixture.put("client", "org.apache.rocketmq:rocketmq-client:5.5.1");
        fixture.put("sourceCommit", "efa1be59d6fc747ad220f036c86030f05f0419f9");
        fixture.put("entrypoint", "NetworkUtil.normalizeHostAddress/UtilAll.isInternalIP/isInternalV6IP/InetAddress.isLoopbackAddress/NettyRemotingClient.getHostAndPort");
        fixture.put("serverBaselineContacted", false);
        fixture.put("addresses", addresses);
        fixture.put("clientAddressCases", clientAddressCases);
        String json = com.alibaba.fastjson2.JSON.toJSONString(fixture) + "\n";
        Path file = Path.of(args[0]);
        if (args.length > 1 && "--verify".equals(args[1])) {
            if (!json.equals(Files.readString(file))) throw new IllegalStateException("fixture drift: " + file);
        } else {
            Files.createDirectories(file.getParent());
            Files.writeString(file, json);
        }
        System.out.println("Runtime address helpers verified: " + addresses.size()
            + "; Java client endpoint splits verified: " + clientAddressCases.size());
    }

    private static void add(List<Object> addresses, String literal, int scope, InetAddress ip) {
        Map<String, Object> item = new TreeMap<>();
        item.put("ip", literal); item.put("scope", scope);
        item.put("internal", ip instanceof Inet6Address ? UtilAll.isInternalV6IP(ip) : UtilAll.isInternalIP(ip.getAddress()));
        item.put("loopback", ip.isLoopbackAddress());
        item.put("normalized", NetworkUtil.normalizeHostAddress(ip));
        addresses.add(item);
    }

    private static final class AddressProbe extends org.apache.rocketmq.remoting.netty.NettyRemotingClient {
        private AddressProbe() {
            super(new org.apache.rocketmq.remoting.netty.NettyClientConfig());
        }

        private String[] splitAddress(String address) {
            return getHostAndPort(address);
        }
    }
}
