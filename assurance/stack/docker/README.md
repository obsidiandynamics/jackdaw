Kafka + ZooKeeper Docker Image
===
Conveniently combines ZooKeeper and Kafka into a single image.

# Running
## Example: vanilla broker setup
```sh
docker run --name kafka --rm -it -p 2181:2181 -p 9092:9092 obsidiandynamics/kafka
```

## Example: advertising the listener on an alternate hostname
Advertising on `kafka:9092` rather than the default `localhost:9092`:
```sh
docker run --name kafka --rm -it -p 2181:2181 -p 9092:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 \
    obsidiandynamics/kafka
```

## Example: multiple advertised listeners
Advertising on `kafka:29092` and `localhost:2092`:
```sh
docker run --name kafka --rm -it -p 2181:2181 -p 9092:9092 -p 29092:29092 \
    -e KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092 \
    -e KAFKA_ADVERTISED_LISTENERS=INTERNAL://kafka:29092,EXTERNAL://localhost:9092 \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL \
    obsidiandynamics/kafka
```

# Configuring
Configuration is administered through environment variables.

|Variable                              |Default            |Description
|:-------------------------------------|:------------------|:----------------
|`KAFKA_BROKER_ID`                     |`0`                |The broker ID (set to -1 to generate a unique ID)
|`KAFKA_LISTENERS`                     |`PLAINTEXT://:9092`|Address(es) the socket server listens on (comma-separated)
|`KAFKA_ADVERTISED_LISTENERS`          |The value of `KAFKA_LISTENERS`|Hostname/port combination(s) the broker will advertise to clients (comma-separated)
|`KAFKA_LISTENER_SECURITY_PROTOCOL_MAP`|`PLAINTEXT:PLAINTEXT,SSL:SSL,SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL`|Maps listener names to security protocols
|`KAFKA_INTER_BROKER_LISTENER_NAME`    |`PLAINTEXT`        |The listener to use for inter-broker communications
|`KAFKA_ADVERTISED_HOST_NAME`          |The canonical hostname of the machine|The advertised hostname (deprecated, prefer KAFKA_ADVERTISED_LISTENERS instead)
|`KAFKA_ADVERTISED_PORT`               |The value of the bound port|The advertised port (deprecated, prefer KAFKA_ADVERTISED_LISTENERS instead)
|`KAFKA_ZOOKEEPER_CONNECT`             |`127.0.0.1:2181 `  |ZooKeeper connect address
|`KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT`  |`10000`            |ZooKeeper connect timeout
|`KAFKA_ZOOKEEPER_SESSION_TIMEOUT`     |`6000`             |ZooKeeper session timeout
|`KAFKA_RESTART_ATTEMPTS`              |`3`                |Number of times to restart the broker if it terminates with a non-zero exit code
|`KAFKA_RESTART_DELAY`                 |`5`                |Number of seconds between restart attempts
|`ZOOKEEPER_AUTOPURGE_PURGE_INTERVAL`  |`0`                |ZooKeeper auto-purge interval (in hours, set to 0 to disable)

# Building
1. Modify the `Dockerfile` as needed 
2. Update the contents of the `version` file to be `{kafka-version}-{buildstamp}`
3. Run `./build`
4. Run `docker push` with the resulting tags