package com.obsidiandynamics.jackdaw;

import org.apache.kafka.clients.admin.*;

final class TestTopic {
  private TestTopic() {}
  
  static NewTopic newOf(String topic) {
    return new NewTopic(topic, 1, (short) 1);
  }
}
