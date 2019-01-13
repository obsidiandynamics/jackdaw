package com.obsidiandynamics.jackdaw;

import com.obsidiandynamics.yconf.*;

@Y
public final class ConsumerPipeConfig {
  @YInject
  private boolean async = true;
  
  @YInject
  private int backlogBatches = 128;
  
  public boolean isAsync() {
    return async;
  }
  
  public void setAsync(boolean async) {
    this.async = async;
  }

  public ConsumerPipeConfig withAsync(boolean async) {
    setAsync(async);
    return this;
  }
  
  public int getBacklogBatches() {
    return backlogBatches;
  }
  
  public void setBacklogBatches(int backlogBatches) {
    this.backlogBatches = backlogBatches;
  }
  
  public ConsumerPipeConfig withBacklogBatches(int backlogBatches) {
    setBacklogBatches(backlogBatches);
    return this;
  }
  
  @Override
  public String toString() {
    return ConsumerPipeConfig.class.getSimpleName() + " [async=" + async + ", backlogBatches=" + backlogBatches + "]";
  }
}
