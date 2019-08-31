package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;

import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.nodequeue.*;
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

public final class ProducerPipe<K, V> implements Terminable, Joinable {
  private static final int MAX_YIELDS = 100;
  private static final int QUEUE_BACKOFF_MILLIS = 1;
  
  private static class AsyncRecord<K, V> {
    final ProducerRecord<K, V> record;
    final Callback callback;
    
    AsyncRecord(ProducerRecord<K, V> record, Callback callback) {
      this.record = record;
      this.callback = callback;
    }
  }
  
  private final NodeQueue<AsyncRecord<K, V>> queue;
  
  private final QueueConsumer<AsyncRecord<K, V>> queueConsumer;
  
  private final Producer<K, V> producer;
  
  private final WorkerThread thread;
  
  private final Retry retry;
  
  private int yields;
  
  private volatile boolean producerDisposed;
  
  public ProducerPipe(ProducerPipeConfig config, Producer<K, V> producer, String threadName, ExceptionHandler exceptionHandler) {
    mustExist(config, "Config cannot be null").validate();
    mustExist(exceptionHandler, "Exception handler cannot be null");
    this.producer = mustExist(producer, "Producer cannot be null");
    this.retry = new Retry()
        .withFaultHandler(exceptionHandler)
        .withErrorHandler(exceptionHandler)
        .withAttempts(config.getSendAttempts());
    
    if (config.isAsync()) {
      mustExist(threadName, "Thread name cannot be null");
      queue = new NodeQueue<>();
      queueConsumer = queue.consumer();
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(threadName))
          .onCycle(this::cycle)
          .buildAndStart();
    } else {
      queue = null;
      queueConsumer = null;
      thread = null;
    }
  }
  
  /**
   *  Pushes a record and an optional send callback through the pipeline.
   *  
   *  @param record The record.
   *  @param callback The callback (may be {@code null}).
   */
  public void send(ProducerRecord<K, V> record, Callback callback) {
    if (thread != null) {
      queue.add(new AsyncRecord<>(record, callback));
    } else {
      sendNow(record, callback);
    }
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    final AsyncRecord<K, V> rec = queueConsumer.poll();
    if (rec != null) {
      sendNow(rec.record, rec.callback);
      yields = 0;
    } else if (yields++ < MAX_YIELDS) {
      Thread.yield();
    } else {
      Thread.sleep(QUEUE_BACKOFF_MILLIS);
    }
  }
  
  private void sendNow(ProducerRecord<K, V> record, Callback callback) {
    try {
      retry.run(() -> {
        try {
          producer.send(record, callback);
        } catch (RuntimeException e) {
          if (! producerDisposed) {
            throw new ProducerException(String.format("Error sending %s", record), e);
          }
        }
      });
    } catch (RuntimeException e) {
      if (callback != null) {
        callback.onCompletion(null, e);
      }
    }
  }
  
  @Override
  public Joinable terminate() {
    Terminator.blank().add(Optional.ofNullable(thread)).terminate();
    closeProducer();
    return this;
  }
  
  void closeProducer() {
    producerDisposed = true;
    producer.close();
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return Joiner.blank().add(Optional.ofNullable(thread)).join(timeoutMillis);
  }
}
