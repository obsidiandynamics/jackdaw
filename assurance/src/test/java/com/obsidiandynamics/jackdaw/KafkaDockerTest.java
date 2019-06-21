package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.net.*;
import java.util.*;

import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;

import com.obsidiandynamics.dockercompose.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.shell.*;
import com.obsidiandynamics.zerolog.*;

@RunWith(Parameterized.class)
public final class KafkaDockerTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }
  
  private ServerSocket socket;
  
  @After
  public void after() throws IOException {
    if (socket != null) {
      socket.close();
      socket = null;
    }
  }
  
  @Test
  public void testGetComposeSingleton() {
    final KafkaDocker kd = new KafkaDocker()
        .withBrokerAwaitMillis(10)
        .withComposeFile("compose.yaml")
        .withPort(9092)
        .withProject("proj")
        .withSink(System.out::print);
    
    final DockerCompose c0 = kd.getCompose();
    assertNotNull(c0);
    
    final DockerCompose c1 = kd.getCompose();
    assertSame(c0, c1);
  }
  
  @Test
  public void testIsRemotePortListeningYes() throws IOException {
    try (ServerSocket socket = randomSocket()) {
      final KafkaDocker kd = new KafkaDocker().withPort(socket.getLocalPort());
      assertTrue(kd.isRunning());
    }
  }
  
  @Test
  public void testIsRemotePortListeningNo() {
    final int sparePort = getSparePort();
    final boolean isRunning = new KafkaDocker().withPort(sparePort).isRunning();
    assertFalse(isRunning);
  }
  
  @Test
  public void testStartAlreadyRunning() throws Exception {
    final MockLogTarget target = new MockLogTarget();
    final Shell shell = mock(Shell.class);
    
    final String host = "127.0.0.1";
    final int port;
    try (ServerSocket socket = randomSocket()) {
      port = socket.getLocalPort();
      final KafkaDocker kd = new KafkaDocker()
          .withLog(target.logger())
          .withHost(host)
          .withPort(port)
          .withShell(shell);
      kd.start();
    }
    
    verifyNoMoreInteractions(shell);
    target.entries().forLevel(LogLevel.INFO).containing("Starting Kafka").assertCount(1);
    target.entries().forLevel(LogLevel.INFO).containing("Broker already running [" + host + ":" + port + "]").assertCount(1);
  }
  
  @Test
  public void testStartNewBroker() throws Exception {
    final int sparePort = getSparePort();
    final MockLogTarget target = new MockLogTarget();
    final ProcessExecutor executor = mock(ProcessExecutor.class);
    final Process process = mock(Process.class);
    when(executor.run(any())).thenReturn(process);
    when(process.getInputStream()).thenAnswer(__ -> {
      socket = socket(sparePort);
      return new ByteArrayInputStream("cruizin'".getBytes());
    });
    final Sink sink = mock(Sink.class);
    
    final KafkaDocker kd = new KafkaDocker()
        .withLog(target.logger())
        .withPort(sparePort)
        .withExecutor(executor)
        .withShell(NullShell.getIntance())
        .withBrokerAwaitMillis(1_000)
        .withSink(sink);
    kd.start();
    
    target.entries().forLevel(LogLevel.INFO).containing("Starting Kafka").assertCount(1);
    target.entries().forLevel(LogLevel.INFO).containing("Container took").assertCount(1);
    target.entries().forLevel(LogLevel.INFO).containing("Broker up").assertCount(1);
    verify(sink, atLeastOnce()).accept(isNotNull());
  }
  
  @Test
  public void testStopAlreadyStopped() throws Exception {
    final int sparePort = getSparePort();
    final MockLogTarget target = new MockLogTarget();
    final Shell shell = mock(Shell.class);
    
    final KafkaDocker kd = new KafkaDocker()
        .withLog(target.logger())
        .withPort(sparePort)
        .withShell(shell);
    kd.stop();
    
    verifyNoMoreInteractions(shell);
    target.entries().forLevel(LogLevel.INFO).containing("Stopping Kafka").assertCount(1);
    target.entries().forLevel(LogLevel.INFO).containing("Broker already stopped").assertCount(1);
  }
  
  @Test
  public void testStopExistingBroker() throws Exception {
    socket = randomSocket();
    final MockLogTarget target = new MockLogTarget();
    final ProcessExecutor executor = mock(ProcessExecutor.class);
    final Process process = mock(Process.class);
    when(executor.run(any())).thenReturn(process);
    when(process.getInputStream()).thenAnswer(__ -> {
      socket.close();
      return new ByteArrayInputStream("cruizin'".getBytes());
    });
    final Sink sink = mock(Sink.class);
    
    final KafkaDocker kd = new KafkaDocker()
        .withLog(target.logger())
        .withPort(socket.getLocalPort())
        .withExecutor(executor)
        .withShell(NullShell.getIntance())
        .withBrokerAwaitMillis(1_000)
        .withSink(sink);
    kd.stop();
    
    target.entries().forLevel(LogLevel.INFO).containing("Stopping Kafka").assertCount(1);
    target.entries().forLevel(LogLevel.INFO).containing("Broker down").assertCount(1);
    verify(sink, atLeastOnce()).accept(isNotNull());
  }
  
  private static int getSparePort() {
    return Exceptions.wrap(() -> {
      try (ServerSocket socket = randomSocket()) {
        return socket.getLocalPort();
      }
    }, RuntimeException::new);
  }
  
  private static ServerSocket randomSocket() {
    return socket(0);
  }
  
  private static ServerSocket socket(int port) {
    return Exceptions.wrap(() -> {
      final ServerSocket socket = new ServerSocket(port);
      socket.setReuseAddress(true);
      return socket;
    }, RuntimeException::new);
  }
}
