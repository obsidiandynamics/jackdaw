package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XDescribeTopicsResult extends DescribeTopicsResult {
  public XDescribeTopicsResult(Map<String, KafkaFuture<TopicDescription>> futures) {
    super(futures);
  }
}
