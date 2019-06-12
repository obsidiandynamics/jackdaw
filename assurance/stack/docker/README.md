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

# Building
Typically if upgrading to a new Kafka/ZK version.

1. Modify the `Dockerfile` as needed 

2. Update the contents of the `version` file to be `{kafka-version}-{buildstamp}`

3. Run `./build`

4. Run `docker push` with the resulting tags