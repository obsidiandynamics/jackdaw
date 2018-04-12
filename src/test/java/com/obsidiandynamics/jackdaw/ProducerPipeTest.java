package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.junit.*;
import org.slf4j.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.jackdaw.SerdeProps.*;
import com.obsidiandynamics.threads.*;

public final class ProducerPipeTest {
  private final Timesert wait = Timesert.wait(10_000);
  
  private ProducerPipe<String, String> pipe;
  
  @After
  public void after() {
    if (pipe != null) pipe.terminate().joinSilently();
  }
  
  @Test
  public void testSendDisposedAsync() {
    final Logger log = mock(Logger.class);
    final Kafka<String, String> kafka = new MockKafka<>();
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, ProducerPipe.class.getSimpleName(), log);

    pipe.closeProducer();
    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    pipe.send(rec, null);
    
    Threads.sleep(10);
    verifyNoMoreInteractions(log);
  }
  
  @Test
  public void testSendAsync() {
    final Logger log = mock(Logger.class);
    final Kafka<String, String> kafka = new MockKafka<>();
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, ProducerPipe.class.getSimpleName(), log);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("test"));
    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    Threads.sleep(10); // give the thread an opportunity to yield and sleep
    pipe.send(rec, null);
    
    wait.until(() -> {
      assertEquals(1, consumer.poll(1).count());
    });
  }
  
  @Test
  public void testSendSync() {
    final Logger log = mock(Logger.class);
    final Kafka<String, String> kafka = new MockKafka<>();
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, ProducerPipe.class.getSimpleName(), log);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("test"));
    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    pipe.send(rec, null);
    
    wait.until(() -> {
      assertEquals(1, consumer.poll(1).count());
    });
  }
  
  @Test
  public void testSendError() {
    final Logger log = mock(Logger.class);
    final Kafka<String, String> kafka = new MockKafka<String, String>()
        .withSendRuntimeExceptionGenerator(r -> new RuntimeException("testSendError"));
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, ProducerPipe.class.getSimpleName(), log);

    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    pipe.send(rec, null);
    
    wait.until(() -> {
      verify(log).error(any(), (Throwable) any());
    });
  }
}
