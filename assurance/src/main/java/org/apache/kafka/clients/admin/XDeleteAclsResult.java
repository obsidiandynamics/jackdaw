package org.apache.kafka.clients.admin;

import java.util.*;

import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.errors.*;

public final class XDeleteAclsResult extends DeleteAclsResult {
  /**
   * A class containing either the deleted ACL binding or an exception if the delete failed.
   */
  public final static class XFilterResult extends DeleteAclsResult.FilterResult {
    public XFilterResult(AclBinding binding, ApiException exception) {
      super(binding, exception);
    }
  }

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
