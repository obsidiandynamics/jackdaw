Jackdaw
===
Simple configuration and mocking of Kafka clients.

[![Download](https://api.bintray.com/packages/obsidiandynamics/jackdaw/jackdaw-core/images/download.svg) ](https://bintray.com/obsidiandynamics/jackdaw/jackdaw-core/_latestVersion)
[![Build](https://travis-ci.org/obsidiandynamics/jackdaw.svg?branch=master) ](https://travis-ci.org/obsidiandynamics/jackdaw#)
[![codecov](https://codecov.io/gh/obsidiandynamics/jackdaw/branch/master/graph/badge.svg)](https://codecov.io/gh/obsidiandynamics/jackdaw)

# Why Jackdaw?
While Kafka is an awesome message streaming platform, it's also a little on the heavyweight side — requiring a cluster of brokers at all times. This makes rapid development and component/integration testing harder than it should be; firstly, you must have access to a broker; secondly, connection establishment and topic rebalancing times can be substantial, blowing out the test times. If only you could simulate the entire Kafka infrastructure in a JVM so that messages can be published and consumed without relying on Kafka. After all, you want to know that _your_ application components integrate correctly; you aren't trying to test Kafka.

Jackdaw is primarily focused on fixing the one area in which Kafka isn't so great — mocking. While it doesn't simulate Kafka in its entirety, it lets you do most things within the mock. It also provides a factory interface — `Kafka` — that lets you easily swap from a mock to a real Kafka connection, playing well with dependency injection.

Jackdaw also makes the Java clients a little nicer to use. The traditional `Consumer` API requires continuous calls to `poll()` from a loop. Jackdaw introduces `AsyncReceiver` — a background polling thread that will invoke your nominated callback handler when messages are received.

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

final ErrorHandler errorHandler = cause -> cause.printStackTrace();

final AsyncReceiver<String, String> receiver = new AsyncReceiver<>(consumer, 1000, "AsyncReceiver", recordHandler, errorHandler);

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

**Note:** As part of the bootstrapping process we can override any of the properties declared in the config. This is very useful when your application relies on specific Kafka behaviour that cannot be supplied by the user. If the user does attempt to set the property in the configuration file, it will be overridden.

If instead of a real Kafka client you need to use a mock, change the configuration to this one-liner:

```yaml
type: com.obsidiandynamics.jackdaw.MockKafka
```

## Producer and consumer pipelining


## Admin
`KafkaAdmin`

# Docker and Kubernetes