package com.obsidiandynamics.jackdaw.sample;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.shell.*;

public final class KafkaDockerSample {
  public static final class Start {
    public static void main(String[] args) throws Exception {
      new KafkaDocker()
      .withProject("jackdaw")
      .withComposeFile("stack/docker-compose/docker-compose.yaml")
      .withShell(new BourneShell().withPath("/usr/local/bin"))
      .withSink(System.out::print)
      .start();
    }
  }
  
  public static final class Stop {
    public static void main(String[] args) throws Exception {
      new KafkaDocker()
      .withProject("jackdaw")
      .withComposeFile("stack/docker-compose/docker-compose.yaml")
      .withShell(new BourneShell().withPath("/usr/local/bin"))
      .withSink(System.out::print)
      .stop();
    }
  }
}
