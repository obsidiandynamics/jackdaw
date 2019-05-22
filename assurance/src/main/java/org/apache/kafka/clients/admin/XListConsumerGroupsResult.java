package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.internals.*;

public final class XListConsumerGroupsResult extends ListConsumerGroupsResult {
  public XListConsumerGroupsResult(KafkaFutureImpl<Collection<Object>> future) {
    super(future);
  }
}
