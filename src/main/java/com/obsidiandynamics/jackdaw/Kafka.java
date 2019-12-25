package com.obsidiandynamics.jackdaw;

import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.func.*;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

public interface Kafka<K, V> {
  default Producer<K, V> getProducer(Properties overrides) {
    return getProducer(new Properties(), overrides);
  }
  
  Producer<K, V> getProducer(Properties defaults, Properties overrides);
  
  Producer<K, V> getProducer(Properties overrides, Serializer<K> keySerializer, Serializer<V> valueSerializer);

  void describeProducer(LogLine logLine, Properties defaults, Properties overrides);
  
  default Consumer<K, V> getConsumer(Properties overrides) {
    return getConsumer(new Properties(), overrides);
  }
  
  Consumer<K, V> getConsumer(Properties overrides, Deserializer<K> keyDeserializer, Deserializer<V> valueDeserializer);

  Consumer<K, V> getConsumer(Properties defaults, Properties overrides);
  
  void describeConsumer(LogLine logLine, Properties defaults, Properties overrides);
  
  AdminClient getAdminClient();
}
