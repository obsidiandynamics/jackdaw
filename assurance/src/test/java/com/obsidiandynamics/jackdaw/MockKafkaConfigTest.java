package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import java.io.*;

import org.junit.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.yconf.*;

public final class MockKafkaConfigTest {
  @Test
  public void testMockConfig() throws IOException {
    final Kafka<?, ?> kafka = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(MockKafkaConfigTest.class.getClassLoader().getResourceAsStream("kafka-mock.conf"))
        .map(Kafka.class);
    assertNotNull(kafka);
    assertEquals(MockKafka.class, kafka.getClass());
    Assertions.assertToStringOverride(kafka);
  }
}
