FROM openjdk:8-jre-alpine

ARG KAFKA_VERSION=2.1.0
ARG SCALA_VERSION=2.12

VOLUME ["/var/lib/kafka"]

# generate unique ID by default
ENV KAFKA_BROKER_ID=-1
ENV KAFKA_ADVERTISED_HOST_NAME=localhost
ENV KAFKA_ADVERTISED_PORT=9092
ENV KAFKA_ZOOKEEPER_CONNECT=127.0.0.1:2181
ENV KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT=10000
ENV KAFKA_ZOOKEEPER_SESSION_TIMEOUT=6000
# number of times to restart the broker if it terminates with a non-zero exit code
ENV KAFKA_RESTART_ATTEMPTS=3
# number of seconds between restart attempts
ENV KAFKA_RESTART_DELAY=5

ENV ZOOKEEPER_AUTOPURGE_PURGE_INTERVAL=0

EXPOSE 2181/tcp 9092/tcp

RUN wget --quiet "http://www.apache.org/dyn/closer.cgi?action=download&filename=/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz" -O /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    wget --quiet https://www.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.asc -P /tmp/ && \
    wget --quiet https://kafka.apache.org/KEYS -P /tmp/ && \
    apk add --no-cache gnupg && \
    gpg2 --import /tmp/KEYS && \
    gpg2 --verify /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.asc /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    gpgconf --kill gpg-agent && \
    apk del --purge gnupg && \
    rm -r ~/.gnupg && \
    mkdir /opt && \
    tar -C /opt -zxf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz && \
    rm /tmp/KEYS /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz.asc && \
    ln -s kafka_${SCALA_VERSION}-${KAFKA_VERSION} /opt/kafka && \
    sed -i 's|^log.dirs=.*$|log.dirs=/var/lib/kafka/logs|' /opt/kafka/config/server.properties && \
    sed -i "s|^dataDir=.*$|dataDir=/var/lib/kafka/zk|" /opt/kafka/config/zookeeper.properties && \
    # for kafka scripts
    apk add --no-cache bash

COPY kafkaw /opt/kafka/bin
RUN chmod +x /opt/kafka/bin/kafkaw

CMD /opt/kafka/bin/kafkaw
