package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.requests.DescribeLogDirsResponse.*;

public final class XDescribeLogDirsResult extends DescribeLogDirsResult {
  public XDescribeLogDirsResult(Map<Integer, KafkaFuture<Map<String, LogDirInfo>>> futures) {
    super(futures);
  }
}
