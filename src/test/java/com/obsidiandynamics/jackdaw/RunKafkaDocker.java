package com.obsidiandynamics.jackdaw;

public final class RunKafkaDocker {
  public static final class Start {
    public static void main(String[] args) throws Exception {
      KafkaDocker.start();
    }
  }
  
  public static final class Stop {
    public static void main(String[] args) throws Exception {
      KafkaDocker.stop();
    }
  }
}
