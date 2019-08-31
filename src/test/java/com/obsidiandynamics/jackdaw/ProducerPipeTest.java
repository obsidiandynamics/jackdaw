package com.obsidiandynamics.jackdaw;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.junit.*;
import org.mockito.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.threads.*;

public final class ProducerPipeTest {
  private static Producer<String, String> mockProducer() {
    return Classes.cast(mock(Producer.class, Answers.CALLS_REAL_METHODS));
  }
  
  private final Timesert wait = Timesert.wait(10_000);
  
  private ProducerPipe<String, String> pipe;
  
  @After
  public void after() {
    if (pipe != null) {
      pipe.terminate().joinSilently();
    }
  }
  
  @Test
  public void testSend_sync_noCallback_success() {
    final Producer<String, String> producer = mockProducer();
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, null, eh);
    final ProducerRecord<String, String> rec = new ProducerRecord<>("topic", "value");
    
    pipe.send(rec, null);
    verify(eh, never()).onException(any(), any());
    verify(producer).send(eq(rec), isNull());
  }
  
  @Test
  public void testSend_sync_withCallback_success() {
    final Producer<String, String> producer = mockProducer();
    when(producer.send(any(), any())).thenAnswer(invocation -> {
      final Callback callback = invocation.getArgument(1);
      callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), null);
      return null;
    });
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, null, eh);
    final ProducerRecord<String, String> rec = new ProducerRecord<>("topic", "value");
    final Callback callback = mock(Callback.class);
    
    pipe.send(rec, callback);
    verify(eh, never()).onException(any(), any());
    verify(producer).send(eq(rec), eq(callback));
    verify(callback).onCompletion(isNotNull(), isNull());
  }
  
  private static final class TestRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }
  
  @Test
  public void testSend_sync_withCallback_errorAfterDispose() {
    final Producer<String, String> producer = mockProducer();
    final TestRuntimeException ex = new TestRuntimeException();
    when(producer.send(any(), any())).thenAnswer(invocation -> {
      throw ex;
    });
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false), producer, null, eh);
    final ProducerRecord<String, String> rec = new ProducerRecord<>("topic", "value");
    final Callback callback = mock(Callback.class);
    pipe.terminate();
    
    pipe.send(rec, callback);
    verify(eh, never()).onException(any(), any());
    verify(producer).send(eq(rec), eq(callback));
    verify(callback, never()).onCompletion(any(), any());
  }
  
  @Test
  public void testSend_sync_noCallback_errorWithRetries() {
    final Producer<String, String> producer = mockProducer();
    final TestRuntimeException ex = new TestRuntimeException();
    when(producer.send(any(), any())).thenAnswer(invocation -> {
      throw ex;
    });
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false).withSendAttempts(2), producer, null, eh);
    final ProducerRecord<String, String> rec = new ProducerRecord<>("topic", "value");
    
    pipe.send(rec, null);
    verify(eh).onException(eq("Fault (attempt #1 of 2): retrying in 100 ms"), isA(ProducerException.class));
    verify(eh).onException(eq("Fault (attempt #2 of 2): aborting"), isA(ProducerException.class));
    verify(producer, times(2)).send(eq(rec), isNull());
  }
  
  @Test
  public void testSend_sync_withCallback_errorWithRetries() {
    final Producer<String, String> producer = mockProducer();
    final TestRuntimeException ex = new TestRuntimeException();
    when(producer.send(any(), any())).thenAnswer(invocation -> {
      throw ex;
    });
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(false).withSendAttempts(2), producer, null, eh);
    final ProducerRecord<String, String> rec = new ProducerRecord<>("topic", "value");
    final Callback callback = mock(Callback.class);
    
    pipe.send(rec, callback);
    verify(eh).onException(eq("Fault (attempt #1 of 2): retrying in 100 ms"), isA(ProducerException.class));
    verify(eh).onException(eq("Fault (attempt #2 of 2): aborting"), isA(ProducerException.class));
    verify(producer, times(2)).send(eq(rec), eq(callback));
    verify(callback).onCompletion(any(), isA(ProducerException.class));
  }
  
  @Test
  public void testSend_async_withCallback_success() {
    final Producer<String, String> producer = mockProducer();
    when(producer.send(any(), any())).thenAnswer(invocation -> {
      final Callback callback = invocation.getArgument(1);
      callback.onCompletion(new RecordMetadata(new TopicPartition("topic", 0), 0L, 0L, 0L, 0L, 0, 0), null);
      return null;
    });
    final ExceptionHandler eh = mock(ExceptionHandler.class);
    pipe = new ProducerPipe<>(new ProducerPipeConfig().withAsync(true), producer, "producer", eh);
    final ProducerRecord<String, String> rec = new ProducerRecord<>("topic", "value");
    final Callback callback = mock(Callback.class);
    
    Threads.sleep(100); // allow time for yields
    pipe.send(rec, callback);
    
    wait.until(() -> {
      verify(eh, never()).onException(any(), any());
      verify(producer).send(eq(rec), eq(callback));
      verify(callback).onCompletion(isNotNull(), isNull());
    });
  }
}
