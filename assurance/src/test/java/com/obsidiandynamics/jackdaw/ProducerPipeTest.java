package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
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
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    final Kafka<String, String> kafka = new MockKafka<>();
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, ProducerPipe.class.getSimpleName(), eh);

    pipe.closeProducer();
    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    pipe.send(rec, null);
    
    Threads.sleep(10);
    verifyNoMoreInteractions(eh);
  }
  
  @Test
  public void testSendAsync() {
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    final Kafka<String, String> kafka = new MockKafka<>();
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, ProducerPipe.class.getSimpleName(), eh);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("test"));
    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    Threads.sleep(50); // give the thread an opportunity to yield and sleep
    pipe.send(rec, null);
    
    wait.until(() -> {
      assertEquals(1, consumer.poll(Duration.ofMillis(1)).count());
    });
  }
  
  @Test
  public void testSendSync() {
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    final Kafka<String, String> kafka = new MockKafka<>();
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, ProducerPipe.class.getSimpleName(), eh);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("test"));
    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    pipe.send(rec, null);
    
    wait.until(() -> {
      assertEquals(1, consumer.poll(Duration.ofMillis(1)).count());
    });
  }
  
  @Test
  public void testSendError() {
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    final RuntimeException cause = new RuntimeException("testSendError");
    final Kafka<String, String> kafka = new MockKafka<String, String>()
        .withSendRuntimeExceptionGenerator(r -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, ProducerPipe.class.getSimpleName(), eh);

    final String msg = "B100";
    final ProducerRecord<String, String> rec = new ProducerRecord<>("test", msg);
    pipe.send(rec, null);
    
    wait.until(() -> {
      verify(eh).onException(isNotNull(), eq(cause));
    });
  }
}
