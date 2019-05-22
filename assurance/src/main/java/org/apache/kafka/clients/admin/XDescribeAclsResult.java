package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;

public final class XDescribeAclsResult extends DescribeAclsResult {
  public XDescribeAclsResult(KafkaFuture<Collection<AclBinding>> future) {
    super(future);
  }
}
