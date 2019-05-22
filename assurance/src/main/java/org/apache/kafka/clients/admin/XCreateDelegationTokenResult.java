package org.apache.kafka.clients.admin;

import org.apache.kafka.common.*;
import org.apache.kafka.common.security.token.delegation.*;

public final class XCreateDelegationTokenResult extends CreateDelegationTokenResult {
  public XCreateDelegationTokenResult(KafkaFuture<DelegationToken> delegationToken) {
    super(delegationToken);
  }
}
