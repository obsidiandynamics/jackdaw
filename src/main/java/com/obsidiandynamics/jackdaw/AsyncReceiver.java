package com.obsidiandynamics.jackdaw;

import java.time.*;

import org.apache.kafka.clients.consumer.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.worker.*;

public final class AsyncReceiver<K, V> implements Terminable, Joinable {
  @FunctionalInterface
  public interface RecordHandler<K, V> {
    void onReceive(ConsumerRecords<K, V> records) throws InterruptedException;
  }
  
  private final Consumer<K, V> consumer;
  
  private final int pollTimeoutMillis;
  
  private final RecordHandler<K, V> recordHandler;
  
  private final ExceptionHandler exceptionHandlerHandler;
  
  private final WorkerThread thread;
  
  public AsyncReceiver(Consumer<K, V> consumer, int pollTimeoutMillis, String threadName, 
                       RecordHandler<K, V> recordHandler, ExceptionHandler exceptionHandler) {
    this.consumer = consumer;
    this.pollTimeoutMillis = pollTimeoutMillis;
    this.recordHandler = recordHandler;
    this.exceptionHandlerHandler = exceptionHandler;
    thread = WorkerThread.builder()
        .withOptions(new WorkerOptions().daemon().withName(threadName))
        .onCycle(this::cycle)
        .onShutdown(this::shutdown)
        .buildAndStart();
  }
  
  private void cycle(WorkerThread thread) throws InterruptedException {
    try {
      final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(pollTimeoutMillis));
      recordHandler.onReceive(records);
    } catch (InterruptedException e) {
      throw e;
    } catch (org.apache.kafka.common.errors.InterruptException e) {
      throw new InterruptedException("Converted from " + org.apache.kafka.common.errors.InterruptException.class.getName());
    } catch (Throwable e) {
      exceptionHandlerHandler.onException("Unexpected error", e);
    }
  }
  
  private void shutdown(WorkerThread thread, Throwable exception) {
    consumer.close();
  }
  
  @Override
  public Joinable terminate() {
    return thread.terminate();
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return thread.join(timeoutMillis);
  }
}
