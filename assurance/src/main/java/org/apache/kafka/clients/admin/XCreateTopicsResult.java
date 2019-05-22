package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XCreateTopicsResult extends CreateTopicsResult {
  public XCreateTopicsResult(Map<String, KafkaFuture<Void>> futures) {
    super(futures);
  }
}
