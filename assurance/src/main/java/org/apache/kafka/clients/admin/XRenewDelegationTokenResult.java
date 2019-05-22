package org.apache.kafka.clients.admin;

import org.apache.kafka.common.*;

public final class XRenewDelegationTokenResult extends RenewDelegationTokenResult {
  public XRenewDelegationTokenResult(KafkaFuture<Long> expiryTimestamp) {
    super(expiryTimestamp);
  }
}
