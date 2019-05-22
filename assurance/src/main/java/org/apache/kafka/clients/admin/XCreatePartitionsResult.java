package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;

public final class XCreatePartitionsResult extends CreatePartitionsResult {
  public XCreatePartitionsResult(Map<String, KafkaFuture<Void>> values) {
    super(values);
  }
}
