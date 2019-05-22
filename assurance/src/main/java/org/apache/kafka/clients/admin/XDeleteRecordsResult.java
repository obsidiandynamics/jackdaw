package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XDeleteRecordsResult extends DeleteRecordsResult {
  public XDeleteRecordsResult(Map<TopicPartition, KafkaFuture<DeletedRecords>> futures) {
    super(futures);
  }
}
