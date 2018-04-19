Jackdaw
===
Simple configuration and mocking of Kafka clients.

[![Download](https://api.bintray.com/packages/obsidiandynamics/jackdaw/jackdaw-core/images/download.svg) ](https://bintray.com/obsidiandynamics/jackdaw/jackdaw-core/_latestVersion)
[![Build](https://travis-ci.org/obsidiandynamics/jackdaw.svg?branch=master) ](https://travis-ci.org/obsidiandynamics/jackdaw#)
[![codecov](https://codecov.io/gh/obsidiandynamics/jackdaw/branch/master/graph/badge.svg)](https://codecov.io/gh/obsidiandynamics/jackdaw)

# Why Jackdaw?
While Kafka is an awesome message streaming platform, it's also a little on the heavy side — requiring a cluster of brokers at all times. This makes rapid development and component/integration testing harder than it should be; firstly, you must have access to a broker; secondly, connection establishment and topic rebalancing times can be substantial, blowing out the build/test times. If only you could simulate the entire Kafka infrastructure in a JVM so that messages can be published and consumed without relying on Kafka. After all, you just want to know that _your_ application components integrate correctly; you aren't trying to test Kafka.

Enter Jackdaw.

Jackdaw is focused on fixing the one area in which Kafka isn't so great — mocking. While it doesn't simulate Kafka in its entirety, it lets you do most things within the mock. It also provides a factory interface — `Kafka` — that lets you easily swap from a mock to a real Kafka connection, playing well with dependency injection.

Jackdaw also makes the Java clients a little nicer to use, without obfuscating the native API. The traditional `Consumer` API requires continuous calls to `poll()` from a loop. Jackdaw introduces `AsyncReceiver` — a background polling thread that will invoke your nominated callback handler when messages are received. Speaking of threading, Jackdaw is also capable of pipelining message (de)serialisation, improving Kafka's performance on multi-core processors.

Bootstrapping is also simplified. Jackdaw offers [YConf](https://github.com/obsidiandynamics/yconf)-ready wrappers for configuring most aspects of the client.

# Dependencies
Gradle builds are hosted on JCenter. Add the following snippet to your build file, replacing `x.y.z` with the version shown on the Download badge at the top of this README.

```groovy
compile "com.obsidiandynamics.jackdaw:jackdaw-core:x.y.z"
```

# Scenarios
## Configuring a real Kafka connection
The following snippet publishes a message and consumes it using a real Kafka connection. You'll need a real broker to run this code.

```java
final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
final Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));

final Properties producerProps = new PropsBuilder()
    .with("key.serializer", StringSerializer.class.getName())
    .with("value.serializer", StringSerializer.class.getName())
    .build();
final Producer<String, String> producer = kafka.getProducer(producerProps);

final Properties consumerProps = new PropsBuilder()
    .with("key.deserializer", StringDeserializer.class.getName())
    .with("value.deserializer", StringDeserializer.class.getName())
    .with("group.id", "group")
    .with("auto.offset.reset", "earliest")
    .with("enable.auto.commit", true)
    .build();
final Consumer<String, String> consumer = kafka.getConsumer(consumerProps);
consumer.subscribe(Collections.singleton("topic"));

zlg.i("Publishing record");
producer.send(new ProducerRecord<>("topic", "key", "value"));

for (;;) {
  final ConsumerRecords<String, String> records = consumer.poll(1000);
  zlg.i("Got %d records", z -> z.arg(records::count));
}
```

**Note:** We use [ZeroLog](https://github.com/obsidiandynamics/zerolog) in our examples for low-overhead logging.

The code above closely resembles how you would normally acquire a `Producer`/`Consumer` pair. The only material difference is that we use the `Kafka` factory interface, which exposes `getProducer(Properties)` and `getConsumer(Properties)` methods. Because we're using real Kafka brokers, we have to supply a `KafkaClusterConfig` with `bootstrapServers` set.

## Configuring a mock Kafka connection
So far there was nothing much to write home about. Jackdaw's real power is unleashed when we swap `KafkaCluster` with `MockKafka`.

```java
final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
final Kafka<String, String> kafka = new MockKafka<>();

final Properties producerProps = new PropsBuilder()
    .with("key.serializer", StringSerializer.class.getName())
    .with("value.serializer", StringSerializer.class.getName())
    .build();
final Producer<String, String> producer = kafka.getProducer(producerProps);

final Properties consumerProps = new PropsBuilder()
    .with("group.id", "group")
    .build();
final Consumer<String, String> consumer = kafka.getConsumer(consumerProps);
consumer.subscribe(Collections.singleton("topic"));

zlg.i("Publishing record");
producer.send(new ProducerRecord<>("topic", "key", "value"));

for (;;) {
  final ConsumerRecords<String, String> records = consumer.poll(1000);
  zlg.i("Got %d records", z -> z.arg(records::count));
}
```

The code is essentially the same. (We did strip out a bunch of unused properties.) It also behaves identically to the previous example. Only now there is no Kafka.

## Asynchronous consumption
Jackdaw provides `AsyncReceiver` — a convenient background worker that continuously polls Kafka for messages. Example below.

```java
final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
final Kafka<String, String> kafka = new MockKafka<>();

final Properties producerProps = new PropsBuilder()
    .with("key.serializer", StringSerializer.class.getName())
    .with("value.serializer", StringSerializer.class.getName())
    .build();
final Producer<String, String> producer = kafka.getProducer(producerProps);

final Properties consumerProps = new PropsBuilder()
    .with("group.id", "group")
    .build();

final Consumer<String, String> consumer = kafka.getConsumer(consumerProps);
consumer.subscribe(Collections.singleton("topic"));

final RecordHandler<String, String> recordHandler = records -> {
  zlg.i("Got %d records", z -> z.arg(records::count));
};

final ExceptionHandler exceptionHandler = ExceptionHandler.forPrintStream(System.err);
final AsyncReceiver<String, String> receiver = 
    new AsyncReceiver<>(consumer, 1000, "AsyncReceiverThread", recordHandler, exceptionHandler);

zlg.i("Publishing record");
producer.send(new ProducerRecord<>("topic", "key", "value"));

Threads.sleep(10_000);

producer.close();
receiver.terminate();
```

The example above used a `MockKafka` factory. It could have just as easily used the real `KafkaCluster`. The `AsyncReceiver` class is completely independent of the underlying `Kafka` implementation.

**Note:** You could, of course, write your own polling loop and stick into into a daemon thread; however, `AsyncReceiver` will already do everything you need, as well as manage the lifecycle of the underlying `Consumer` instance. When `AsyncReceiver` terminates, it will automatically clean up after itself and close the `Consumer`. It also exposes the `terminate()` lifecycle method, allowing you to interrupt the receiver and terminate the poll loop.

## Configuration
Another common use case is configuration. In the following example we use [YConf](https://github.com/obsidiandynamics/yconf) to bootstrap a Kafka client from a YAML file.

```java
final Kafka<?, ?> kafka = new MappingContext()
    .withParser(new SnakeyamlParser())
    .fromStream(KafkaConfigTest.class.getClassLoader().getResourceAsStream("kafka-cluster.conf"))
    .map(Kafka.class);

// override the properties from the config
final Properties overrides = new PropsBuilder()
    .with("max.in.flight.requests.per.connection", 1)
    .build();

final Producer<?, ?> producer = kafka.getProducer(overrides);
// do stuff with the producer
// ...
producer.close();
```

The configuration file is shown below.

```yaml
type: com.obsidiandynamics.jackdaw.KafkaCluster
clusterConfig:
  common:
    bootstrap.servers: localhost:9092
  producer:
    acks: 1
    retries: 0
    batch.size: 16384
    linger.ms: 0
    buffer.memory: 33554432
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
  consumer:
    enable.auto.commit: false
    auto.commit.interval.ms: 0
```

If instead of a real Kafka client you need to use a mock, change the configuration to this one-liner:

```yaml
type: com.obsidiandynamics.jackdaw.MockKafka
```

As part of the bootstrapping process we can override any of the properties declared in the config. This is very useful when your application relies on specific Kafka behaviour that cannot be supplied by the user. In the example above, the property `max.in.flight.requests.per.connection` is not user-editable. If the user does attempt to set the property in the configuration file, it will be overridden.

In a slightly more elaborate example, suppose there are a set of default properties that your application relies upon that may be overridden by the user, but you don't want to rely on their presence in the configuration file. (In case the user deletes them.) The example below illustrates the use of default properties when instantiating a producer.

```java
final Kafka<?, ?> kafka = new MappingContext()
    .withParser(new SnakeyamlParser())
    .fromStream(KafkaConfigTest.class.getClassLoader().getResourceAsStream("kafka-cluster.conf"))
    .map(Kafka.class);

// default properties
final Properties defaults = new PropsBuilder()
    .with("compression.type", "lz4")
    .build();

// override the properties from the config
final Properties overrides = new PropsBuilder()
    .with("max.in.flight.requests.per.connection", 1)
    .build();

final Producer<?, ?> producer = kafka.getProducer(defaults, overrides);
// do stuff with the producer
// ...
producer.close();
```

**Note:** The use of defaults and overrides does not alter the underlying configuration; it only applies to the individual construction of `Producer` and `Consumer` instances.

## Producer and consumer pipelining
Another area where Kafka disappoints is the (lack of) pipelining of message (de)serialization. Calling `Consumer.poll()` and `Producer.send()` will result in the (de)serialization of messages in the foreground (the calling thread). In other words, your application threads are performing socket I/O between the client and the broker, mapping between byte streams and POJOs, as well as executing business logic.

Interleaving CPU-bound and I/O bound operations within the same thread isn't considered a good practice; a missed opportunity to capitalise on parallelism, especially with multi-core hardware. Jackdaw comes with two classes — `ProducerPipe` and `ConsumerPipe` — offering configurable pipelining of message serialization and deserialization. The example below illustrates their use.

```java
final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
final Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));

final Properties producerProps = new PropsBuilder()
    .with("key.serializer", StringSerializer.class.getName())
    .with("value.serializer", StringSerializer.class.getName())
    .build();
final Producer<String, String> producer = kafka.getProducer(producerProps);

final Properties consumerProps = new PropsBuilder()
    .with("key.deserializer", StringDeserializer.class.getName())
    .with("value.deserializer", StringDeserializer.class.getName())
    .with("group.id", "group")
    .with("auto.offset.reset", "earliest")
    .with("enable.auto.commit", true)
    .build();
final Consumer<String, String> consumer = kafka.getConsumer(consumerProps);
consumer.subscribe(Collections.singleton("topic"));

final ExceptionHandler exceptionHandler = ExceptionHandler.forPrintStream(System.err);
final ProducerPipeConfig producerPipeConfig = new ProducerPipeConfig().withAsync(true);
final ProducerPipe<String, String> producerPipe = new ProducerPipe<>(producerPipeConfig, 
    producer, "ProducerPipeThread", exceptionHandler);

zlg.i("Publishing record");
// sending doesn't block, not even to serialize the record
producerPipe.send(new ProducerRecord<>("topic", "key", "value"), null);

final RecordHandler<String, String> recordHandler = records -> {
  zlg.i("Got %d records", z -> z.arg(records::count));
};
final ConsumerPipeConfig consumerPipeConfig = new ConsumerPipeConfig().withAsync(true).withBacklogBatches(128);
final ConsumerPipe<String, String> consumerPipe = new ConsumerPipe<>(consumerPipeConfig, 
    recordHandler, ConsumerPipe.class.getSimpleName());

for (;;) {
  // calling receive() doesn't block (up to the 'backlogBatches' capacity of the underlying queue); 
  // calling poll() blocks as expected
  consumerPipe.receive(consumer.poll(1000));
}
```

Notice that the code is very similar to prior examples, only in this case we wrap `send()` and `poll()` within their respective pipelines. In async mode, polling and deserialization will take place in a different thread from the `RecordHandler`. Conversely, sending to a pipe will return immediately, while the actual serialization and enqueuing will occur in a different thread.

As a further convenience, the asynchronous behaviour of pipe is configurable (on by default). Once a pipeline has been installed, it can easily be bypassed by calling `withAsync(false)` on the `ProducerPipeConfig` and `ConsumerPipeConfig` objects. This effectively short-circuits the pipelines, reverting to synchronous pass-through behaviour.

# Sundries
## Admin
Kafka has introduced an `AdminClient` class in version 0.11, simplifying administrative operations on the broker. The `KafkaAdmin` class converts the asynchronous (`Future`-based) API of `AdminClient` to a blocking model.

`KafkaAdmin` is pretty new, only supporting a small handful of methods at this stage:

* `describeCluster(long timeoutMillis)` — Describes the cluster, blocking until all operations have completed or a timeout occurs.
* `ensureExists(NewTopic topic, long timeoutMillis)` — Ensures that a given topic exists, creating one if necessary. This method blocks until all operations have completed or a timeout occurs.

## Docker and Kubernetes
We typically do our Kafka testing with Docker — either via Docker Compose or Minikube (local Kubernetes). The `${project_root}/stack` directory contains `docker-compose.yaml` and Kubernetes resource files to spin up a small-scale (single-node) ZooKeeper + Kafka cluster locally.

Jackdaw also offers with a handy Java API to spin up a Docker Compose stack for integration tests.

```java
// configure Docker Compose
final KafkaDocker kafkaDocker = new KafkaDocker()
    .withProject("jackdaw")
    .withComposeFile("stack/docker-compose/docker-compose.yaml")
    .withShell(new BourneShell().withPath("/usr/local/bin")) // the path should point to the docker-compose installation
    .withSink(System.out::print);

// spin up a Kafka stack
kafkaDocker.start();

// ... do stuff requiring a real Kafka broker

// bring the stack down (and dispose of any volumes)
kafkaDocker.stop();
```

The recommended approach is to place a copy of `docker-compose.yaml` with your preferred ZK/Kafka composition into your project, and point `KafkaDocker` at your private configuration (in your own repo). You could use the `docker-compose.yaml` file in the Jackdaw repo, but that may change without notice.