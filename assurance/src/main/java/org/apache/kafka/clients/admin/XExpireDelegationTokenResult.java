package org.apache.kafka.clients.admin;

import org.apache.kafka.common.*;

public final class XExpireDelegationTokenResult extends ExpireDelegationTokenResult {
  public XExpireDelegationTokenResult(KafkaFuture<Long> expiryTimestamp) {
    super(expiryTimestamp);
  }
}