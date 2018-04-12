package com.obsidiandynamics.jackdaw;

import com.obsidiandynamics.yconf.*;

@Y
public final class ProducerPipeConfig {
  @YInject
  private boolean async;
  
  public ProducerPipeConfig withAsync(boolean async) {
    this.async = async;
    return this;
  }
  
  boolean isAsync() {
    return async;
  }
  
  @Override
  public String toString() {
    return ProducerPipeConfig.class.getSimpleName() + " [async=" + async + "]";
  }
}
