package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.*;
import org.junit.*;

import com.obsidiandynamics.assertion.*;

public final class RecordDescriptorTest {
  @Test
  public void testForProducerRecord() {
    final ProducerRecord<Integer, String> rec = new ProducerRecord<>("topic", 3, 1000L, 42, "value", Collections.emptyList());
    final RecordDescriptor<Integer, String> desc = RecordDescriptor.forRecord(rec);
    assertEquals("topic", desc.topic());
    assertEquals((Integer) 3, desc.partition());
    assertNull(desc.offset());
    assertEquals((Long) 1000L, desc.timestamp());
    assertEquals((Integer) 42, desc.key());
    assertEquals("value", desc.value());
    assertArrayEquals(new Header[0], desc.headers().toArray());
    Assertions.assertToStringOverride(desc);
  }

  @Test
  public void testForConsumerRecord() {
    final ConsumerRecord<Integer, String> rec = new ConsumerRecord<>("topic", 3, 1000L, 42, "value");
    final RecordDescriptor<Integer, String> desc = RecordDescriptor.forRecord(rec);
    assertEquals("topic", desc.topic());
    assertEquals((Integer) 3, desc.partition());
    assertEquals((Long) 1000L, desc.offset());
    assertEquals((Long) (-1L), desc.timestamp());
    assertEquals((Integer) 42, desc.key());
    assertEquals("value", desc.value());
    assertArrayEquals(new Header[0], desc.headers().toArray());
    Assertions.assertToStringOverride(desc);
  }
}
