# Phase 18 Java 5.5.1 provenance and package review

Rechecked 2026-10-22 on current revision `782a9cdb`.

## Maven reference artifacts

The harness in compat/java-reference/pom.xml sets rocketmq.client.version to
5.5.1. Its offline Maven dependency tree resolves:

    org.apache.rocketmq:rocketmq-client:5.5.1
    └── org.apache.rocketmq:rocketmq-remoting:5.5.1
        └── org.apache.rocketmq:rocketmq-common:5.5.1

Embedded pom.properties files in all three binary JARs report
org.apache.rocketmq and version 5.5.1. For every binary JAR, source JAR, and
POM below, the local Maven-cache file's SHA-1 matched the corresponding
published .sha1 value at:

https://repo.maven.apache.org/maven2/org/apache/rocketmq/{artifact}/5.5.1/{filename}.sha1

| Artifact file | Local SHA-256 | Matching Central SHA-1 |
| --- | --- | --- |
| rocketmq-client-5.5.1.jar | 1c39c94a28e59b292e29d699872d693cdc2ce30bd54c4e0f95a4c377fd9dff84 | 925e018a0f18646f26672efb204c8d6b60b41326 |
| rocketmq-client-5.5.1-sources.jar | de63ae377bcc9b461b0d65b4cb77673d9daabb8f46caabef903826ea7c627e76 | 027faa344864758d12d0fee5029f7e4760dbeca1 |
| rocketmq-client-5.5.1.pom | 1d4e1059040b84487a39374be66a508278710045a9ac0b39cc1abd65a4c46770 | af0b84b9ba8763ffc78809ca914b3b2c40bbb5f4 |
| rocketmq-common-5.5.1.jar | 6a15b581269c0e06d9508d284c84c05a8578ffa1b16db3e60ce837c97f684b97 | c32e72d38da781545bfa06dd5c365f1c6dad2c04 |
| rocketmq-common-5.5.1-sources.jar | c98b42edf50365bf7242d728adc4442295a02924cc34e0bd4fcefb1b2d12e242 | 515a21d0fa1163742db6017fbb5473760d08ad27 |
| rocketmq-common-5.5.1.pom | 5bef6986b208048454f41017f9755d3f1acaf68c9109f1c67e51fb8a4d59fa2e | 216916d445f0b04cccf6e15f9515fcbfa4e3dd53 |
| rocketmq-remoting-5.5.1.jar | a8694e3cf7ea956de1c3a42638e59c4d676f16efb18b11a486f95bfa5b978121 | a166bf3422aef7cfde4cc51493f70f101ce8edbe |
| rocketmq-remoting-5.5.1-sources.jar | ac2d2ad39cbe35e2125ce8cfdd2dd1c20ff7007f9960c1c4b597e292123a6bf6 | 56816d67b5d9b3cb1f1db6eefe4729641bfcab51 |
| rocketmq-remoting-5.5.1.pom | 6f49882a8b53eee9ac674f70aa758a450ab68951b296d672964c4452451ab751 | 3256b7aa743e36c6c5dacc10a8188b8c17f8cba7 |

## Source tag comparison

The external Apache RocketMQ checkout contains annotated tag
rocketmq-all-5.5.1, which resolves to
efa1be59d6fc747ad220f036c86030f05f0419f9. That commit object exists in the
checkout. The following source-JAR entries exactly match their files at that
tag; each row gives the byte-identical content's SHA-256:

| Source file | SHA-256 |
| --- | --- |
| client/src/main/java/org/apache/rocketmq/client/impl/MQClientAPIImpl.java | bfb0ebf8e4b4c0fd4ad5a4db46d64a7c8897250fb411487793a23f29aa094737 |
| client/src/main/java/org/apache/rocketmq/client/impl/consumer/DefaultMQPushConsumerImpl.java | 166fd55dd79596cb2ebfbe0ea39d8f9398d85dace7b872abbc1592f6c22d1712 |
| common/src/main/java/org/apache/rocketmq/common/message/MessageExt.java | 19a256b417a68daac6b1da6fe3b48a9bfa1fef7a9c09a6c8a29fdfb56ae34382 |
| remoting/src/main/java/org/apache/rocketmq/remoting/protocol/body/LockBatchResponseBody.java | 231e5c95588246fbd7eecc77aba03d3367d6bca03dacefe02d2cda1c6c898b89 |

The tag object contains an OpenPGP signature. git tag -v could not verify it
in this environment because no gpg executable is installed. This record
therefore establishes the tag target and source-JAR byte identity, but does not
assert verification of the tag signer.

## ConsumerStatsManager scheduled sampling

Pinned `ConsumerStatsManager` delegates each metric family to its own
`StatsItemSet`. Each set registers 10-second and 10-minute fixed-rate sample
tasks. `getAndCreateItem` does not initialize a new item, so its minute/hour
snapshots are empty until those shared tasks sample it. Java's `consumeStatus`
therefore returns zero before the first sample; after a sample, TPS is derived
from the sampled value delta and elapsed time. Failed-message totals use the
hour window and stay zero until its first 10-minute sample.

The invoked fixture uses a `ScheduledExecutorService` proxy to retain the five
10-second samplers. It records pull TPS as `0.0` before the first sample,
`2.0` after sampling an increment of 20, and `2.0` after a later increment of
5 that has not yet been sampled. Fixture SHA-256:
`57b6f19d68fb3460e7fa86bd6b08bf16f43a03d28cce2558716f88c110ccb369`.
Rust evaluates manager-aligned 10-second and 10-minute ticks lazily; it does
not expose a Rust-only scheduler API. Core tests use deterministic timestamps,
while consumer-flow tests wait for their first real sample when they assert
positive TPS/RT values.

## Rust package graph

Locked Cargo metadata on revision `782a9cdb` resolves 216 packages,
including 208 registry packages and all eight workspace packages. Each
workspace package is version 0.1.0 with publication disabled (`publish =
false`; Cargo metadata represents this as an empty registry allowlist). The
workspace package-content command lists eight packages: seven product crates
and the development-only `rocketmq-classic-integration` crate. Each product
crate listing includes LICENSE and NOTICE; the integration crate is not a
product artifact and is excluded from that check. Protocol uses direct
`indexmap 2.14.2` with Serde support to preserve Java `TopicConfig` attribute
iteration order; metadata/license checks and all seven product package
listings pass after that dependency change. The workspace
metadata/license/dependency-direction checker passes. These checks do not
claim that unpublished path dependencies resolve from a future registry.

The dependency graph's leaf-to-facade order for a future publication is:

1. rocketmq-classic-common
2. rocketmq-classic-protocol
3. rocketmq-classic-remoting
4. rocketmq-classic-core
5. rocketmq-classic-producer
6. rocketmq-classic-consumer
7. rocketmq-classic

The repository baseline identifies Java client 5.5.1 separately from the
Phase 19 Broker 5.3.4 baseline. Confirming the intended release version set,
registry resolution, enabling publication, publishing, and tagging remain
separate post-Phase-19 work.
