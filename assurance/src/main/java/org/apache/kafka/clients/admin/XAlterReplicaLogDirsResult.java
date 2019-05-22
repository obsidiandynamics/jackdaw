package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XAlterReplicaLogDirsResult extends AlterReplicaLogDirsResult {
  public XAlterReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<Void>> futures) {
    super(futures);
  }
}
