package com.obsidiandynamics.jackdaw;

import org.apache.kafka.clients.admin.*;

public final class TestTopic {
  private TestTopic() {}
  
  public static NewTopic newOf(String topic) {
    return new NewTopic(topic, (short) 1, (short) 1);
  }
}
