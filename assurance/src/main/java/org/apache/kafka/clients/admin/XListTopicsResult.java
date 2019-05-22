package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XListTopicsResult extends ListTopicsResult {
  public XListTopicsResult(KafkaFuture<Map<String, TopicListing>> future) {
    super(future);
  }
}
