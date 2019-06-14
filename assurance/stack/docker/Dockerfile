FROM openjdk:8-jre-alpine

ARG KAFKA_VERSION=2.2.1
ARG SCALA_VERSION=2.12

VOLUME ["/var/lib/kafka"]

# The broker ID (set to -1 to generate a unique ID)
# Default: -1
ENV KAFKA_BROKER_ID=""

# Address(es) the socket server listens on (comma-separated)
# Default: PLAINTEXT://:9092
ENV KAFKA_LISTENERS=""

# Hostname/port combination(s) the broker will advertise to clients (comma-separated)
# Default: The value of `KAFKA_LISTENERS`
ENV KAFKA_ADVERTISED_LISTENERS=""

# Maps listener names to security protocols
# Default: PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL
ENV KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=""

# The listener to use for inter-broker communications
# Default: PLAINTEXT
ENV KAFKA_INTER_BROKER_LISTENER_NAME=""

# The advertised hostname (deprecated, prefer KAFKA_ADVERTISED_LISTENERS instead)
# Default: The canonical hostname of the machine
ENV KAFKA_ADVERTISED_HOST_NAME=""

# The advertised port (deprecated, prefer KAFKA_ADVERTISED_LISTENERS instead)
# Default: The value of the bound port
ENV KAFKA_ADVERTISED_PORT=""

# ZooKeeper connect address
# Default: 127.0.0.1:2181
ENV KAFKA_ZOOKEEPER_CONNECT=""

# ZooKeeper connect timeout
# Default: 10000
ENV KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT=""

# ZooKeeper session timeout
# Default: 6000
ENV KAFKA_ZOOKEEPER_SESSION_TIMEOUT=""

# Number of times to restart the broker if it terminates with a non-zero exit code
# Default: 3
ENV KAFKA_RESTART_ATTEMPTS=""

# Number of seconds between restart attempts
# Default: 5
ENV KAFKA_RESTART_DELAY=""

# ZooKeeper auto-purge interval (in hours, set to 0 to disable)
# Default: 0
ENV ZOOKEEPER_AUTOPURGE_PURGE_INTERVAL=""

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
COPY version /opt/kafka
RUN chmod +x /opt/kafka/bin/kafkaw

CMD /opt/kafka/bin/kafkaw
