package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.junit.*;

import com.obsidiandynamics.await.*;
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.func.*;

public final class ConsumerPipeTest {
  private final Timesert wait = Timesert.wait(10_000);
  
  private ConsumerPipe<String, String> pipe;
  
  @After
  public void after() {
    if (pipe != null) pipe.terminate().joinSilently();
  }
  
  private static ConsumerRecords<String, String> records(ConsumerRecord<String, String> record) {
    return new ConsumerRecords<>(Collections.singletonMap(new TopicPartition(record.topic(), record.partition()), 
                                                          Collections.singletonList(record)));
  }
  
  @Test
  public void testReceive_async() throws InterruptedException {
    final RecordHandler<String, String> handler = Classes.cast(mock(RecordHandler.class));
    pipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(true), handler, ConsumerPipe.class.getSimpleName());
    
    final String msg = "B100";
    final ConsumerRecords<String, String> records = records(new ConsumerRecord<>("test", 0, 0, "key", msg));
    assertTrue(pipe.receive(records));
    
    wait.until(() -> {
      try {
        verify(handler).onReceive(eq(records));
      } catch (InterruptedException e) {
        throw new AssertionError("Unexpected exception", e);
      }
    });
  }
  
  @Test
  public void testReceive_asyncEmpty() throws InterruptedException {
    final RecordHandler<String, String> handler = Classes.cast(mock(RecordHandler.class));
    pipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(true), handler, ConsumerPipe.class.getSimpleName());
    
    final ConsumerRecords<String, String> records = new ConsumerRecords<>(Collections.emptyMap());
    assertTrue(pipe.receive(records));
    
    Thread.sleep(10);
    try {
      verify(handler, never()).onReceive(any());
    } catch (InterruptedException e) {
      throw new AssertionError("Unexpected exception", e);
    }
  }
  
  @Test
  public void testReceive_sync() throws InterruptedException {
    final RecordHandler<String, String> handler = Classes.cast(mock(RecordHandler.class));
    pipe = new ConsumerPipe<>(new ConsumerPipeConfig().withAsync(false), handler, ConsumerPipe.class.getSimpleName());
    
    final String msg = "B100";
    final ConsumerRecords<String, String> records = records(new ConsumerRecord<>("test", 0, 0, "key", msg));
    assertTrue(pipe.receive(records));
    
    verify(handler).onReceive(eq(records));
  }
}
