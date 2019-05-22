package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;

public final class XCreateAclsResult extends CreateAclsResult {
  public XCreateAclsResult(Map<AclBinding, KafkaFuture<Void>> futures) {
    super(futures);
  }
}
