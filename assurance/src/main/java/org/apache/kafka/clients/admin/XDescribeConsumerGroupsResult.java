package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XDescribeConsumerGroupsResult extends DescribeConsumerGroupsResult {
  public XDescribeConsumerGroupsResult(final Map<String, KafkaFuture<ConsumerGroupDescription>> futures) {
    super(futures);
  }
}
