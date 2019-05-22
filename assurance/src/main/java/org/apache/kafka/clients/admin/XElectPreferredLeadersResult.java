package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.internals.*;
import org.apache.kafka.common.requests.*;

public final class XElectPreferredLeadersResult extends ElectPreferredLeadersResult {
  public XElectPreferredLeadersResult(KafkaFutureImpl<Map<TopicPartition, ApiError>> electionFuture, Set<TopicPartition> partitions) {
    super(electionFuture, partitions);
  }
}
