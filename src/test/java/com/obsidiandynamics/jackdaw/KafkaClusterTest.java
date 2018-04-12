package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;
import org.junit.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.yconf.props.*;

public class KafkaClusterTest {
  private static <K, V> KafkaCluster<K, V> createCluster() {
    final KafkaClusterConfig config = new KafkaClusterConfig()
        .withCommonProps(new PropsBuilder()
                         .with("bootstrap.servers", "localhost:9092")
                         .build())
        .withProducerProps(new PropsBuilder()
                           .with("key.serializer", StringSerializer.class.getName())
                           .with("value.serializer", StringSerializer.class.getName())
                           .build())
        .withConsumerProps(new PropsBuilder()
                           .with("key.deserializer", StringDeserializer.class.getName())
                           .with("value.deserializer", StringDeserializer.class.getName())
                           .build());
    return new KafkaCluster<>(config);
  }
  
  @Test
  public void testGetProducer() {
    try (Producer<?, ?> producer = createCluster().getProducer(new Properties())) {
      assertNotNull(producer);
    }
  }
  
  @Test
  public void testDescribeProducer() {
    final LogLine logLine = mock(LogLine.class);
    createCluster().describeProducer(logLine, 
                                     new PropsBuilder().with("default", "1").build(),
                                     new PropsBuilder().with("overriden", "2").build());
    verify(logLine, atLeastOnce()).accept(any());
  }
  
  @Test
  public void testGetConsumer() {
    try (Consumer<?, ?> producer = createCluster().getConsumer(new Properties())) {
      assertNotNull(producer);
    }
  }
  @Test
  public void testDescribeConsumer() {
    final LogLine logLine = mock(LogLine.class);
    createCluster().describeConsumer(logLine, 
                                     new PropsBuilder().with("default", "1").build(),
                                     new PropsBuilder().with("overriden", "2").build());
    verify(logLine, atLeastOnce()).accept(any());
  }
}
