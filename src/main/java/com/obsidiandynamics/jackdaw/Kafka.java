package com.obsidiandynamics.jackdaw;

import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.func.*;

public interface Kafka<K, V> {
  default Producer<K, V> getProducer(Properties overrides) {
    return getProducer(new Properties(), overrides);
  }
  
  Producer<K, V> getProducer(Properties defaults, Properties overrides);
  
  void describeProducer(LogLine logLine, Properties defaults, Properties overrides);
  
  default Consumer<K, V> getConsumer(Properties overrides) {
    return getConsumer(new Properties(), overrides);
  }
  
  Consumer<K, V> getConsumer(Properties defaults, Properties overrides);
  
  void describeConsumer(LogLine logLine, Properties defaults, Properties overrides);
  
  AdminClient getAdminClient();
}
