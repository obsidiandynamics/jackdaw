package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;


public final class XDescribeClusterResult extends DescribeClusterResult {
  public XDescribeClusterResult(KafkaFuture<Collection<Node>> nodes,
                                KafkaFuture<Node> controller,
                                KafkaFuture<String> clusterId,
                                KafkaFuture<Set<AclOperation>> authorizedOperations) {
    super(nodes, controller, clusterId, authorizedOperations);
  }
}
