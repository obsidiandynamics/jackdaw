package com.obsidiandynamics.jackdaw;

import static java.util.Arrays.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;
import org.junit.*;
import org.junit.runners.*;

import com.obsidiandynamics.jackdaw.KafkaAdmin.*;
import com.obsidiandynamics.yconf.util.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class KafkaAdminDockerIT {
  private static final long TIMESTAMP = System.currentTimeMillis();
  private static final String TOPIC_A = getUniqueName("A");
  private static final int TOPIC_A_PARTITIONS = 4;
  private static final String TOPIC_B = getUniqueName("B");
  private static final int TOPIC_B_PARTITIONS = 1;
  private static final String GROUP = getUniqueName("G");

  private static final int DEF_TIMEOUT = 10_000;

  private static final KafkaClusterConfig CONFIG = new KafkaClusterConfig().withBootstrapServers("localhost:9092");

  private static String getUniqueName(String suffix) {
    return getPrefix() + "-" + TIMESTAMP + "-" + suffix;
  }

  private static String getPrefix() {
    return KafkaAdminDockerIT.class.getSimpleName();
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    new KafkaDocker().start();
  }

  @Test
  public void _00_testListAndDeleteTopics() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final Set<String> allTopics = admin.listTopics(DEF_TIMEOUT);
      final String testTopicPrefix = getPrefix();
      final List<String> testTopics = allTopics.stream().filter(topic -> topic.startsWith(testTopicPrefix)).collect(Collectors.toList());
      final Set<String> deleted = admin.deleteTopics(testTopics, DEF_TIMEOUT);
      assertThat(deleted).containsExactlyInAnyOrderElementsOf(testTopics);
    }
  }

  @Test
  public void _01_testCreateTopics() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final Set<String> created = admin.createTopics(asList(TestTopic.newOf(TOPIC_A, TOPIC_A_PARTITIONS), 
                                                            TestTopic.newOf(TOPIC_B, TOPIC_B_PARTITIONS)), 
                                                     DEF_TIMEOUT);
      assertThat(created).containsExactlyInAnyOrder(TOPIC_A, TOPIC_B);
    }
  }

  @Test
  public void _02_testCreateDuplicateTopics() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.of(new KafkaCluster<>(CONFIG).getAdminClient())) {
      final Set<String> created = admin.createTopics(asList(TestTopic.newOf(TOPIC_A), TestTopic.newOf(TOPIC_B)), DEF_TIMEOUT);
      assertThat(created).isEmpty();
    }
  }

  @Test
  public void _03_testListAndDeleteConsumerGroups() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final Set<String> allConsumerGroups = admin.listConsumerGroups(DEF_TIMEOUT);
      final String testConsumerGroupPrefix = getPrefix();
      final List<String> testConsumerGroups = allConsumerGroups.stream().filter(group -> group.startsWith(testConsumerGroupPrefix)).collect(Collectors.toList());
      final Set<String> deleted = admin.deleteConsumerGroups(testConsumerGroups, DEF_TIMEOUT);
      assertThat(deleted).containsExactlyInAnyOrderElementsOf(testConsumerGroups);
    }
  }

  @Test
  public void _04_testPublishAndConsume() {
    final KafkaCluster<String, String> kafkaCluster = new KafkaCluster<>(CONFIG);
    final Properties producerProps = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .withSystemDefault("linger.ms", 0)
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .build();
    try (Producer<String, String> producer = kafkaCluster.getProducer(producerProps)) {
      producer.send(new ProducerRecord<String, String>(TOPIC_A, "someMessage"));
    }

    final Properties consumerProps = new PropsBuilder()
        .with("group.id", GROUP)
        .with("auto.offset.reset", OffsetResetStrategy.EARLIEST.name().toLowerCase())
        .with("enable.auto.commit", true)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", StringDeserializer.class.getName())
        .build();
    try (Consumer<String, String> consumer = kafkaCluster.getConsumer(consumerProps)) {
      consumer.subscribe(Collections.singleton(TOPIC_A));
      for (;;) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        if (! records.isEmpty()) {
          consumer.commitSync();
          break;
        }
      }
    }
  }

  @Test
  public void _05_testListConsumerGroupOffsets() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(GROUP, DEF_TIMEOUT);
      final TopicPartition[] expectedTopics = IntStream.range(0, 3).boxed().map(i -> new TopicPartition(TOPIC_A, i)).toArray(TopicPartition[]::new);
      assertThat(offsets).containsKeys(expectedTopics);
    }
  }

  @Test
  public void _06_testListAndDeleteConsumerGroups() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final Set<String> allConsumerGroups = admin.listConsumerGroups(DEF_TIMEOUT);
      final String testConsumerGroupPrefix = getPrefix();
      final List<String> testConsumerGroups = allConsumerGroups.stream().filter(group -> group.startsWith(testConsumerGroupPrefix)).collect(Collectors.toList());
      final Set<String> deleted = admin.deleteConsumerGroups(testConsumerGroups, DEF_TIMEOUT);
      assertThat(deleted).containsExactlyInAnyOrderElementsOf(testConsumerGroups);
    }
  }

  @Test
  public void _07_testDeleteTopics() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final Set<String> deleted = admin.deleteTopics(asList(TOPIC_A, TOPIC_B), DEF_TIMEOUT);
      assertThat(deleted).containsExactlyInAnyOrder(TOPIC_A, TOPIC_B);
    }
  }

  @Test
  public void _08_describeCluster() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(CONFIG, AdminClient::create)) {
      final DescribeClusterOutcome outcome = admin.describeCluster(DEF_TIMEOUT);
      assertNotNull(outcome);
    }
  } 
}
