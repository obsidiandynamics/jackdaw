package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;

public final class XDeleteAclsResult extends DeleteAclsResult {
  /**
   * A class containing the results of the delete ACLs operation.
   */
  public final static class XFilterResults extends DeleteAclsResult.FilterResults {
    public XFilterResults(List<FilterResult> values) {
      super(values);
    }
  }

  public XDeleteAclsResult(Map<AclBindingFilter, KafkaFuture<FilterResults>> futures) {
    super(futures);
  }
}
