package com.obsidiandynamics.jackdaw;

import com.obsidiandynamics.yconf.*;

@Y
public final class ConsumerPipeConfig {
  @YInject
  private boolean async = true;
  
  @YInject
  private int backlogBatches = 128;
  
  public ConsumerPipeConfig withAsync(boolean async) {
    this.async = async;
    return this;
  }
  
  boolean isAsync() {
    return async;
  }
  
  public ConsumerPipeConfig withBacklogBatches(int backlogBatches) {
    this.backlogBatches = backlogBatches;
    return this;
  }
  
  int getBacklogBatches() {
    return backlogBatches;
  }
  
  @Override
  public String toString() {
    return ConsumerPipeConfig.class.getSimpleName() + " [async=" + async + ", backlogBatches=" + backlogBatches + "]";
  }
}
