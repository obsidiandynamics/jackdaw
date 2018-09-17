package com.obsidiandynamics.jackdaw;

import static org.assertj.core.api.Assertions.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

import org.apache.kafka.clients.admin.*;
import org.junit.*;
import org.junit.runners.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class KafkaAdminDockerIT {
  private static final long TIMESTAMP = System.currentTimeMillis();
  private static final String TOPIC_A = getTopicName("A");
  private static final String TOPIC_B = getTopicName("B");
  
  private static final int DEF_TIMEOUT = 10_000;
  
  private static String getTopicName(String suffix) {
    return getTopicPrefix() + "-" + TIMESTAMP + "-" + suffix;
  }
  
  private static String getTopicPrefix() {
    return KafkaAdminDockerIT.class.getSimpleName();
  }
  
  private final KafkaClusterConfig config = new KafkaClusterConfig().withBootstrapServers("localhost:9092");
  
  @BeforeClass
  public static void beforeClass() throws Exception {
    new KafkaDocker().start();
  }
  
  @Test
  public void _00_testListAndDeleteTopics() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      final Set<String> allTopics = admin.listTopics(DEF_TIMEOUT);
      final String testTopicPrefix = getTopicPrefix();
      final List<String> testTopics = allTopics.stream().filter(topic -> topic.startsWith(testTopicPrefix)).collect(Collectors.toList());
      final Set<String> deleted = admin.deleteTopics(testTopics, 10_000);
      assertThat(deleted).containsExactlyInAnyOrderElementsOf(testTopics);
    }
  }
  
  @Test
  public void _01_testCreateTopics() throws ExecutionException, TimeoutException, InterruptedException {
    try (KafkaAdmin admin = KafkaAdmin.forConfig(config, AdminClient::create)) {
      final Set<String> created = admin.createTopics(Arrays.asList(TestTopic.newOf(TOPIC_A), TestTopic.newOf(TOPIC_B)), DEF_TIMEOUT);
      assertThat(created).containsExactlyInAnyOrder(TOPIC_A, TOPIC_B);
    }
  }
}
