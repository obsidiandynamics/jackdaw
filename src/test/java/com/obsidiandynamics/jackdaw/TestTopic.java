package com.obsidiandynamics.jackdaw;

import org.apache.kafka.clients.admin.*;

final class TestTopic {
  private TestTopic() {}
  
  static NewTopic newOf(String topic) {
    return newOf(topic, 1);
  }
  
  static NewTopic newOf(String topic, int numPartitions) {
    return newOf(topic, numPartitions, (short) 1);
  }
  
  static NewTopic newOf(String topic, int numPartitions, short replicationFactor) {
    return new NewTopic(topic, numPartitions, replicationFactor);
  }
}
