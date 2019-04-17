Build Instructions
===
Typically if upgrading to a new Kafka/ZK version.

1. Modify the `Dockerfile` as needed 

2. Update the contents of the `version` file to be `{kafka-version}-{buildstamp}`

3. Run `./build`

4. Run `docker push` with the resulting tags