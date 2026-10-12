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
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.remoting.netty.TlsHelper;
import org.apache.rocketmq.remoting.netty.TlsSystemConfig;

/** Invoked Java 5.5.1 TLS cipher and protocol property oracle. */
public final class TlsSettingsFixtures {
    private static final String SOURCE_COMMIT = "efa1be59d6fc747ad220f036c86030f05f0419f9";

    private TlsSettingsFixtures() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) throw new IllegalArgumentException("expected fixture path");
        Path path = Path.of(args[0]);
        boolean verify = args.length > 1 && "--verify".equals(args[1]);
        String json = JSON.toJSONString(render()) + "\n";
        if (verify) {
            String recorded = Files.exists(path) ? Files.readString(path) : "<missing>";
            if (!json.equals(recorded)) {
                throw new IllegalStateException(
                    "Java TLS settings fixture drift: " + path + "\nrecorded=" + recorded + "\nactual=" + json);
            }
        } else {
            Files.createDirectories(path.getParent());
            Files.writeString(path, json);
        }
    }

    private static Map<String, Object> render() throws Exception {
        String ciphers = TlsSystemConfig.tlsCiphers;
        String protocols = TlsSystemConfig.tlsProtocols;
        Path configFile = Files.createTempFile("rocketmq-tls-settings", ".properties");
        String fileCiphers = "TLS_AES_128_GCM_SHA256";
        String fileProtocols = "TLSv1.3";
        Files.writeString(configFile,
            "tls.ciphers=" + fileCiphers + "\ntls.protocols=" + fileProtocols + "\n");
        try {
            Method extract = TlsHelper.class.getDeclaredMethod("extractTlsConfigFromFile", java.io.File.class);
            extract.setAccessible(true);
            extract.invoke(null, configFile.toFile());
        } finally {
            Files.deleteIfExists(configFile);
        }

        Map<String, Object> settings = new TreeMap<>();
        settings.put("ciphers", ciphers);
        settings.put("protocols", protocols);
        Map<String, Object> effectiveSettings = new TreeMap<>();
        effectiveSettings.put("ciphers", TlsSystemConfig.tlsCiphers);
        effectiveSettings.put("protocols", TlsSystemConfig.tlsProtocols);
        Map<String, Object> fileValues = new TreeMap<>();
        fileValues.put("ciphers", fileCiphers);
        fileValues.put("protocols", fileProtocols);

        String previousConfigFile = TlsSystemConfig.tlsConfigFile;
        String previousCiphers = TlsSystemConfig.tlsCiphers;
        String previousProtocols = TlsSystemConfig.tlsProtocols;
        boolean previousTestMode = TlsSystemConfig.tlsTestModeEnable;
        boolean clientIgnoresCipherProtocolProperties;
        Path emptyConfigFile = Files.createTempFile("rocketmq-tls-client", ".properties");
        try {
            TlsSystemConfig.tlsConfigFile = emptyConfigFile.toString();
            TlsSystemConfig.tlsCiphers = "INVALID_CLIENT_CIPHER";
            TlsSystemConfig.tlsProtocols = "INVALID_CLIENT_PROTOCOL";
            TlsSystemConfig.tlsTestModeEnable = false;
            TlsHelper.buildSslContext(true);
            clientIgnoresCipherProtocolProperties = true;
        } finally {
            TlsSystemConfig.tlsConfigFile = previousConfigFile;
            TlsSystemConfig.tlsCiphers = previousCiphers;
            TlsSystemConfig.tlsProtocols = previousProtocols;
            TlsSystemConfig.tlsTestModeEnable = previousTestMode;
            Files.deleteIfExists(emptyConfigFile);
        }

        Map<String, Object> result = new TreeMap<>();
        result.put("sourceCommit", SOURCE_COMMIT);
        result.put("clientArtifact", "org.apache.rocketmq:rocketmq-client:5.5.1");
        result.put("entrypoints", new String[] {
            "TlsSystemConfig.TLS_CIPHERS/TLS_PROTOCOLS",
            "TlsSystemConfig.tlsCiphers/tlsProtocols",
            "TlsHelper.moreTlsConfig",
            "TlsHelper.extractTlsConfigFromFile",
            "TlsHelper.buildSslContext(true)"
        });
        Map<String, Object> propertyNames = new TreeMap<>();
        propertyNames.put("ciphers", TlsSystemConfig.TLS_CIPHERS);
        propertyNames.put("protocols", TlsSystemConfig.TLS_PROTOCOLS);
        result.put("propertyNames", propertyNames);
        result.put("settings", settings);
        result.put("configFileValues", fileValues);
        result.put("effectiveSettings", effectiveSettings);
        result.put("clientIgnoresCipherProtocolProperties", clientIgnoresCipherProtocolProperties);
        result.put("configFileOverridesSettings",
            !java.util.Objects.equals(ciphers, TlsSystemConfig.tlsCiphers)
                || !java.util.Objects.equals(protocols, TlsSystemConfig.tlsProtocols));
        result.put("serverContacted", false);
        return result;
    }

}
