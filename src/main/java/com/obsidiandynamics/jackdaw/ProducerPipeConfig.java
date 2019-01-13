package com.obsidiandynamics.jackdaw;

import com.obsidiandynamics.yconf.*;

@Y
public final class ProducerPipeConfig {
  @YInject
  private boolean async = true;
  
  public boolean isAsync() {
    return async;
  }
  
  public void setAsync(boolean async) {
    this.async = async;
  }

  public ProducerPipeConfig withAsync(boolean async) {
    setAsync(async);
    return this;
  }
  
  @Override
  public String toString() {
    return ProducerPipeConfig.class.getSimpleName() + " [async=" + async + "]";
  }
}
