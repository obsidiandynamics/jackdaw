package com.obsidiandynamics.jackdaw;

import com.obsidiandynamics.yconf.*;

@Y
public final class ProducerPipeConfig {
  @YInject
  private boolean async = true;
  
  @YInject
  private int sendAttempts = 10;

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
  
  public int getSendAttempts() {
    return sendAttempts;
  }

  public void setSendAttempts(int sendAttempts) {
    this.sendAttempts = sendAttempts;
  }
  
  public ProducerPipeConfig withSendAttempts(int sendAttempts) {
    setSendAttempts(sendAttempts);
    return this;
  }
  
  @Override
  public String toString() {
    return ProducerPipeConfig.class.getSimpleName() + " [async=" + async + ", sendAttempts=" + sendAttempts + "]";
  }
}
