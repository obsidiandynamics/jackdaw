Run `docker-compose up` in the directory where `docker-compose.yaml` is kept.

To connect to the machine, run `docker ps` to determine the container ID of confluent/kafka, then run `docker exec -it <container> sh`

Kafka scripts are located in `/opt/kafka/bin`.

Run test producer:

    ./kafka-console-producer.sh --broker-list `broker-list.sh` --topic test

Type messages at the console, 'enter' to send.

Run test consumer:

    ./kafka-console-consumer.sh --zookeeper zookeeper:2181 --topic test --from-beginning

To create a new topic:

    ./kafka-topics.sh --zookeeper zookeeper --create --topic test --partitions 1 --replication-factor 1

Note: this setup uses a static Kafka port mapping (9092:9092); don't try running `docker-compose scale` on it.

To delete a topic:

    ./kafka-topics --zookeeper zookeeper --delete --topic test

The topic is marked for deletion, and may linger indefinitely as Kafka does not delete topics by default. To force immediate expulsion from ZooKeeper:

    ./zookeeper-shell zookeeper rmr /brokers/topics/test

To enable topic deletion on Kafka edit `/opt/kafka/config/server.properties` and uncomment the line `delete.topic.enable=true`. Restart the broker for this to take effect. This option is better, as deleting a topic will now clean up the log data.
