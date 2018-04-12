package com.obsidiandynamics.jackdaw;

import java.io.*;
import java.lang.invoke.*;
import java.net.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.dockercompose.*;
import com.obsidiandynamics.shell.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaDocker {
  private static final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
  
  private static final String PROJECT = "jackdaw";
  private static final String PATH = "/usr/local/bin";
  private static final int BROKER_AWAIT_MILLIS = 600_000;
  
  private static final DockerCompose COMPOSE = new DockerCompose()
      .withShell(new BourneShell().withPath(PATH))
      .withProject(PROJECT)
      .withEcho(true)
      .withSink(System.out::print)
      .withComposeFile("stack/docker-compose.yaml");
  
  private KafkaDocker() {}
  
  public static boolean isRunning() {
    return isRemotePortListening("localhost", 9092);
  }
  
  private static boolean isRemotePortListening(String host, int port) {
    try (final Socket s = new Socket(host, port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }
  
  public static void start() throws Exception {
    zlg.i("Starting Kafka stack...").log();
    if (isRunning()) {
      zlg.i("Kafka already running").log();
      return;
    }

    COMPOSE.checkInstalled();
    final long tookStartMillis = Threads.tookMillis(COMPOSE::up);
    zlg.i("took %,d ms (now waiting for broker to come up...)").arg(tookStartMillis).log();
    final long tookAwait = awaitBroker();
    zlg.i("broker up, waited %,d ms").arg(tookAwait).log();
  }
  
  private static long awaitBroker() {
    return Threads.tookMillis(() -> Timesert.wait(BROKER_AWAIT_MILLIS).untilTrue(KafkaDocker::isRunning));
  }
  
  public static void stop() throws Exception {
    zlg.i("Stopping Kafka stack...").log();
    if (! isRunning()) {
      zlg.i("Kafka already stopped").log();
      return;
    }
    
    final long tookMillis = Threads.tookMillis(() -> {
      COMPOSE.stop(1);
      COMPOSE.down(true);
    });
    zlg.i("took %,d ms").arg(tookMillis).log();
  }
}
