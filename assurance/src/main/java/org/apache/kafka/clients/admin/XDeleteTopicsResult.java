package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public class XDeleteTopicsResult extends DeleteTopicsResult {
  public XDeleteTopicsResult(Map<String, KafkaFuture<Void>> futures) {
    super(futures);
  }
}
