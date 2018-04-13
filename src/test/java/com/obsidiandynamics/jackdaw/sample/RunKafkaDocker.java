package com.obsidiandynamics.jackdaw.sample;

import com.obsidiandynamics.jackdaw.*;

public final class RunKafkaDocker {
  public static final class Start {
    public static void main(String[] args) throws Exception {
      new KafkaDocker().start();
    }
  }
  
  public static final class Stop {
    public static void main(String[] args) throws Exception {
      new KafkaDocker().stop();
    }
  }
}
