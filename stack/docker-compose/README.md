ZooKeeper/Kafka stack on Docker Compose
===
# Getting started
Run `docker-compose up` in the directory where `docker-compose.yaml` is kept.

To connect to the machine, run `docker ps` to determine the container ID of confluent/kafka, then run `docker exec -it <container> bash`.