package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;


public final class XDescribeClusterResult extends DescribeClusterResult {
  public XDescribeClusterResult(KafkaFuture<Collection<Node>> nodes,
                                KafkaFuture<Node> controller,
                                KafkaFuture<String> clusterId) {
    super(nodes, controller, clusterId);
  }
}
