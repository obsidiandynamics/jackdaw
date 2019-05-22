package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XDeleteConsumerGroupsResult extends DeleteConsumerGroupsResult {
  public XDeleteConsumerGroupsResult(final Map<String, KafkaFuture<Void>> futures) {
    super(futures);
  }
}
