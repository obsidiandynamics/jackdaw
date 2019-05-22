package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.config.*;

public final class XDescribeConfigsResult extends DescribeConfigsResult {
  public XDescribeConfigsResult(Map<ConfigResource, KafkaFuture<Config>> futures) {
    super(futures);
  }
}
