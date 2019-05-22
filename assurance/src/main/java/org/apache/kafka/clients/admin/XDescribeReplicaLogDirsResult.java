package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XDescribeReplicaLogDirsResult extends DescribeReplicaLogDirsResult {
  public static final class XReplicaLogDirInfo extends ReplicaLogDirInfo {
    public XReplicaLogDirInfo(String currentReplicaLogDir,
                              long currentReplicaOffsetLag,
                              String futureReplicaLogDir,
                              long futureReplicaOffsetLag) {
      super(currentReplicaLogDir, currentReplicaOffsetLag, futureReplicaLogDir, futureReplicaOffsetLag);
    }
  }

  public XDescribeReplicaLogDirsResult(Map<TopicPartitionReplica, KafkaFuture<ReplicaLogDirInfo>> futures) {
    super(futures);
  }
}
