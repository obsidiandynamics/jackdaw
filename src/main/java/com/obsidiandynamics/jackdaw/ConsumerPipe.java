package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.func.Functions.*;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;

import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

public final class ConsumerPipe<K, V> implements Terminable, Joinable {
  private final BlockingQueue<ConsumerRecords<K, V>> queue;
  
  private final RecordHandler<K, V> handler;
  
  private final WorkerThread thread;
  
  public ConsumerPipe(ConsumerPipeConfig config, RecordHandler<K, V> handler, String threadName) {
    this.handler = handler;
    if (config.isAsync()) {
      mustExist(threadName, "Thread name cannot be null");
      queue = new LinkedBlockingQueue<>(config.getBacklogBatches());
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(threadName))
          .onCycle(this::cycle)
          .buildAndStart();
    } else {
      queue = null;
      thread = null;
    }
  }
  
  /**
   *  Pushes newly received records through the pipeline.
   *  
   *  @param records The records to push.
   *  @return True if records were enqueued (in async mode). Sync mode, or if the record set is empty 
   *          <em>always</em> returns true.
   *  @throws InterruptedException If this thread was interrupted (only in async mode).
   */
  public boolean receive(ConsumerRecords<K, V> records) throws InterruptedException {
    if (! records.isEmpty()) {
      if (thread != null) {
        return queue.offer(records);
      } else {
        handler.onReceive(records);
        return true;
      }
    } else {
      return true;
    }
  }
  
  private void cycle(WorkerThread t) throws InterruptedException {
    final ConsumerRecords<K, V> records = queue.take();
    handler.onReceive(records);
  }
  
  @Override
  public Joinable terminate() {
    Terminator.blank().add(Optional.ofNullable(thread)).terminate();
    return this;
  }

  @Override
  public boolean join(long timeoutMillis) throws InterruptedException {
    return Joiner.blank().add(Optional.ofNullable(thread)).join(timeoutMillis);
  }
}
