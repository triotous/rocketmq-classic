FROM eclipse-temurin:8-jre@sha256:acfd157ba9dc5b7cbfd2bd2391bc206a6d23e6ddbba564eb89cf60e77de77831

RUN groupadd --gid 3000 rocketmq \
    && useradd --uid 3000 --gid 3000 --home-dir /home/rocketmq --create-home \
        --shell /usr/sbin/nologin rocketmq

COPY --chown=3000:3000 rocketmq-all-5.3.4-bin-release/ /home/rocketmq/rocketmq-5.3.4/
COPY --chown=3000:3000 --chmod=0755 docker-entrypoint.sh /home/rocketmq/rocketmq-5.3.4/bin/docker-entrypoint.sh

ENV ROCKETMQ_HOME=/home/rocketmq/rocketmq-5.3.4
WORKDIR /home/rocketmq/rocketmq-5.3.4/bin
USER 3000:3000
ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["dummy"]
