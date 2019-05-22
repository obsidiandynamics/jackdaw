package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public final class XListConsumerGroupOffsetsResult extends ListConsumerGroupOffsetsResult {
  public XListConsumerGroupOffsetsResult(KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> future) {
    super(future);
  }
}
