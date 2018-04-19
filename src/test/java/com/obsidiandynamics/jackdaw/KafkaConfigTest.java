package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import java.io.*;
import java.util.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.yconf.props.*;

public final class KafkaConfigTest {
  @Test
  public void testClusterConfig() throws IOException {
    final Kafka<?, ?> kafka = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(KafkaConfigTest.class.getClassLoader().getResourceAsStream("kafka-cluster.conf"))
        .map(Kafka.class);
    assertNotNull(kafka);
    assertEquals(KafkaCluster.class, kafka.getClass());
    Assertions.assertToStringOverride(kafka);
    
    final KafkaClusterConfig config = ((KafkaCluster<?, ?>) kafka).getConfig();
    assertNotNull(config);
    assertEquals(new PropsBuilder().with(KafkaClusterConfig.CONFIG_BOOTSTRAP_SERVERS, "localhost:9092").build(),
                 config.getCommonProps());
    Assertions.assertToStringOverride(config);
  }
  
  @Test
  public void testMockConfig() throws IOException {
    final Kafka<?, ?> kafka = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(KafkaConfigTest.class.getClassLoader().getResourceAsStream("kafka-mock.conf"))
        .map(Kafka.class);
    assertNotNull(kafka);
    assertEquals(MockKafka.class, kafka.getClass());
    Assertions.assertToStringOverride(kafka);
  }
  
  @Test
  public void testApi() {
    final Properties commonProps = new PropsBuilder().with("common", "COMMON").build();
    final Properties producerProps = new PropsBuilder().with("producer", "PRODUCER").build();
    final Properties consumerProps = new PropsBuilder().with("consumer", "CONSUMER").build();
    
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withCommonProps(commonProps)
        .withProducerProps(producerProps)
        .withConsumerProps(consumerProps);
    
    assertEquals(commonProps, config.getCommonProps());

    producerProps.putAll(commonProps);
    assertEquals(producerProps, config.getProducerCombinedProps());

    consumerProps.putAll(commonProps);
    assertEquals(consumerProps, config.getConsumerCombinedProps());
  }  
  
  @Test
  public void testBootstrapServers() {
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withBootstrapServers("localhost:9092");
    
    assertEquals("localhost:9092", config.getCommonProps().getProperty(KafkaClusterConfig.CONFIG_BOOTSTRAP_SERVERS));
  }
  
  @Test(expected=IllegalArgumentException.class)
  public void testNoBoostrapServers() {
    new KafkaClusterConfig().validate();
  }
}
