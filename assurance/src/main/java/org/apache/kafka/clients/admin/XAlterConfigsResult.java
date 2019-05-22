package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.config.*;

public final class XAlterConfigsResult extends AlterConfigsResult {
  public XAlterConfigsResult(Map<ConfigResource, KafkaFuture<Void>> futures) {
    super(futures);
  }
}
