package com.obsidiandynamics.jackdaw;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.junit.*;
import org.mockito.stubbing.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.threads.*;


public final class AsyncReceiverTest {
  private AsyncReceiver<String, String> receiver;
  private Consumer<String, String> consumer;
  private RecordHandler<String, String> recordHandler;
  private ExceptionHandler exceptionHandler;
  
  private final Timesert wait = Timesert.wait(10_000);
  
  @Before
  @SuppressWarnings("unchecked")
  public void before() {
    consumer = mock(Consumer.class);
    recordHandler = mock(RecordHandler.class);
    exceptionHandler = mock(ExceptionHandler.class);
  }
  
  @After
  public void after() throws InterruptedException {
    if (receiver != null) receiver.terminate().join();
  }
  
  private static Answer<?> split(Supplier<ConsumerRecords<String, String>> first) {
    return split(first, first);
  }
  
  /**
   *  Generates an answer in a way such that the first invocation returns the result
   *  of the {@code first} supplier, while the second and the rest of the invocation
   *  return the result of {@code second}.
   *  
   *  @param first Supplies the return value of the first invocation.
   *  @param others Supplies the return value of the second invocation.
   *  @return The answer.
   */
  private static Answer<?> split(Supplier<ConsumerRecords<String, String>> first,
                                 Supplier<ConsumerRecords<String, String>> others) {
    final AtomicBoolean firstCall = new AtomicBoolean();
    return invocation -> {
      if (firstCall.compareAndSet(false, true)) {
        return first.get();
      } else {
        final long timeout = (Long) invocation.getArguments()[0];
        Thread.sleep(timeout);
        return others.get();
      }
    };
  }
  
  @Test
  public void testReceive() {
    final Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = 
        Collections.singletonMap(new TopicPartition("test", 0), Arrays.asList(new ConsumerRecord<>("test", 0, 0, "key", "value")));
    final ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
    
    when(consumer.poll(anyLong())).then(split(() -> records, 
                                              () -> new ConsumerRecords<>(Collections.emptyMap())));
    receiver = new AsyncReceiver<String, String>(consumer, 1, "TestThread", recordHandler, exceptionHandler);
    wait.until(() -> {
      try {
        verify(recordHandler, times(1)).onReceive(eq(records));
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      verify(exceptionHandler, never()).onException(any(), any());
    });
  }
  
  @Test
  public void testNoRecords() throws InterruptedException {
    final Map<TopicPartition, List<ConsumerRecord<String, String>>> recordsMap = 
        Collections.emptyMap();
    final ConsumerRecords<String, String> records = new ConsumerRecords<>(recordsMap);
    
    when(consumer.poll(anyLong())).then(split(() -> records, 
                                              () -> new ConsumerRecords<>(Collections.emptyMap())));
    receiver = new AsyncReceiver<String, String>(consumer, 1, "TestThread", recordHandler, exceptionHandler);
    
    Threads.sleep(10);
    verify(recordHandler, never()).onReceive(any());
    verify(exceptionHandler, never()).onException(any(), any());
  }

  @Test
  public void testInterrupt() throws InterruptedException {
    when(consumer.poll(anyLong())).then(split(() -> { throw createInterruptException(); }));
    receiver = new AsyncReceiver<String, String>(consumer, 1, "TestThread", recordHandler, exceptionHandler);
    verify(recordHandler, never()).onReceive(any());
    verify(exceptionHandler, never()).onException(any(), any());
    receiver.join();
  }
  
  @Test
  public void testError() throws InterruptedException {
    final RuntimeException cause = new RuntimeException("boom");
    when(consumer.poll(anyLong())).then(split(() -> { throw cause; }));
    receiver = new AsyncReceiver<String, String>(consumer, 1, "TestThread", recordHandler, exceptionHandler);
    wait.until(() -> {
      try {
        verify(recordHandler, never()).onReceive(any());
      } catch (InterruptedException e) {
        throw new AssertionError(e);
      }
      verify(exceptionHandler, atLeastOnce()).onException(isNotNull(), eq(cause));
    });
    receiver.terminate().join();
    verify(consumer).close();
  }
  
  private static InterruptException createInterruptException() {
    final InterruptException ie = new InterruptException("Interrupted");
    Thread.interrupted();
    return ie;
  }
}
