package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.security.token.delegation.*;

public final class XDescribeDelegationTokenResult extends DescribeDelegationTokenResult {
  public XDescribeDelegationTokenResult(KafkaFuture<List<DelegationToken>> delegationTokens) {
    super(delegationTokens);
  }
}
