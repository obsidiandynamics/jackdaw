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
  private Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
  
  private String project = "jackdaw";
  
  private String composeFile = "stack/docker-compose/docker-compose.yaml";
  
  private Shell shell = new BourneShell().withPath("/usr/local/bin");
  
  private ProcessExecutor executor = new DefaultProcessExecutor();
  
  private Sink sink = System.out::print;
  
  private int brokerAwaitMillis = 600_000;
  
  private int port = 9092;
  
  public KafkaDocker withLog(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }
  
  public KafkaDocker withProject(String project) {
    this.project = project;
    return this;
  }

  public KafkaDocker withComposeFile(String composeFile) {
    this.composeFile = composeFile;
    return this;
  }
  
  public KafkaDocker withShell(Shell shell) {
    this.shell = shell;
    return this;
  }
  
  public KafkaDocker withExecutor(ProcessExecutor executor) {
    this.executor = executor;
    return this;
  }
  
  public KafkaDocker withSink(Sink sink) {
    this.sink = sink;
    return this;
  }

  public KafkaDocker withBrokerAwaitMillis(int brokerAwaitMillis) {
    this.brokerAwaitMillis = brokerAwaitMillis;
    return this;
  }

  public KafkaDocker withPort(int port) {
    this.port = port;
    return this;
  }
  
  private DockerCompose compose = null;
  
  private DockerCompose createCompose() {
    return new DockerCompose()
        .withShell(shell)
        .withExecutor(executor)
        .withProject(project)
        .withEcho(true)
        .withSink(sink)
        .withComposeFile(composeFile);
  }
  
  DockerCompose getCompose() {
    if (compose == null) {
      compose = createCompose();
    }
    return compose;
  }
  
  public boolean isRunning() {
    return isRemotePortListening("localhost", port);
  }
  
  private static boolean isRemotePortListening(String host, int port) {
    try (final Socket s = new Socket(host, port)) {
      s.setReuseAddress(true);
      return true;
    } catch (IOException e) {
      return false;
    }
  }
  
  public void start() throws Exception {
    zlg.i("Starting Kafka stack [%s]...", z -> z.arg(composeFile));
    if (isRunning()) {
      zlg.i("Broker already running [port %d]", z -> z.arg(port));
      return;
    }

    getCompose().checkInstalled();
    final long containerStartupMillis = Threads.tookMillis(getCompose()::up);
    zlg.i("Container took %,d ms; now waiting for broker to come up...", z -> z.arg(containerStartupMillis));
    final long brokerStartupMillis = awaitBroker();
    zlg.i("Broker up [waited %,d ms]", z -> z.arg(brokerStartupMillis));
  }
  
  private long awaitBroker() {
    return Threads.tookMillis(() -> Timesert.wait(brokerAwaitMillis).untilTrue(this::isRunning));
  }
  
  public void stop() throws Exception {
    zlg.i("Stopping Kafka stack...");
    if (! isRunning()) {
      zlg.i("Broker already stopped");
      return;
    }
    
    final long tookMillis = Threads.tookMillis(() -> {
      getCompose().stop(1);
      getCompose().down(true);
    });
    zlg.i("Broker down [waited %,d ms]", z -> z.arg(tookMillis));
  }
}
