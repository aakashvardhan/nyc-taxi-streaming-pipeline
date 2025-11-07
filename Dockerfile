FROM alpine:3.19

ARG KAFKA_VERSION=2.3.0
ARG SCALA_VERSION=2.12
ENV KAFKAHOME=/home
ENV PATH=$KAFKAHOME/kafka/bin:$PATH

RUN apk add --no-cache bash curl tar coreutils openjdk8-jdk python3 py3-pip netcat-openbsd

RUN mkdir -p ${KAFKAHOME} \
    && curl -fSL https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -o /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C ${KAFKAHOME} \
    && mv ${KAFKAHOME}/kafka_${SCALA_VERSION}-${KAFKA_VERSION} ${KAFKAHOME}/kafka \
    && chmod +x ${KAFKAHOME}/kafka/bin/*.sh \
    && rm -f /tmp/kafka.tgz

RUN mkdir -p ${KAFKAHOME}/kafka/data/zookeeper ${KAFKAHOME}/kafka/data/server

RUN sed -i 's|^dataDir=.*|dataDir=/home/kafka/data/zookeeper|' ${KAFKAHOME}/kafka/config/zookeeper.properties \
    && sed -i 's|^zookeeper.connect=.*|zookeeper.connect=zookeeper:2181|' ${KAFKAHOME}/kafka/config/server.properties \
    && sed -i 's|^#\\?log.dirs=.*|log.dirs=/home/kafka/data/server|' ${KAFKAHOME}/kafka/config/server.properties \
    && sed -i 's|^#\\?listeners=.*||' ${KAFKAHOME}/kafka/config/server.properties \
    && sed -i 's|^#\\?advertised.listeners=.*||' ${KAFKAHOME}/kafka/config/server.properties \
    && sed -i 's|^#\\?inter.broker.listener.name=.*||' ${KAFKAHOME}/kafka/config/server.properties \
    && sed -i 's|^#\\?listener.security.protocol.map=.*||' ${KAFKAHOME}/kafka/config/server.properties \
    && printf '\n%s\n' \
    'listeners=PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092' \
    'advertised.listeners=PLAINTEXT://broker:9092,PLAINTEXT_HOST://localhost:29092' \
    'listener.security.protocol.map=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT' \
    'inter.broker.listener.name=PLAINTEXT' \
    >> ${KAFKAHOME}/kafka/config/server.properties

RUN adduser -D -h /home/kafka kafka && chown -R kafka:kafka /home/kafka

RUN printf '%s\n' '#!/bin/sh' \
    'rm -f /home/kafka/data/server/.lock' \
    'exec /home/kafka/bin/kafka-server-start.sh /home/kafka/config/server.properties' \
    > /usr/local/bin/kafka-start && chmod +x /usr/local/bin/kafka-start

RUN python3 -m venv /home/kafka/venv
ENV PATH="/home/kafka/venv/bin:$PATH"

RUN mkdir -p /home/kafka/app

USER kafka
WORKDIR /home/kafka
