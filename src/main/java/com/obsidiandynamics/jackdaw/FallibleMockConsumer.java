package com.obsidiandynamics.jackdaw;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public abstract class FallibleMockConsumer<K, V> extends MockConsumer<K, V> {
  protected ExceptionGenerator<Map<TopicPartition, OffsetAndMetadata>, Exception> commitExceptionGenerator = ExceptionGenerator.never();
  
  FallibleMockConsumer(OffsetResetStrategy offsetResetStrategy) {
    super(offsetResetStrategy);
  }
  
  public void setCommitExceptionGenerator(ExceptionGenerator<Map<TopicPartition, OffsetAndMetadata>, Exception> commitExceptionGenerator) {
    this.commitExceptionGenerator = commitExceptionGenerator;
  }
}
