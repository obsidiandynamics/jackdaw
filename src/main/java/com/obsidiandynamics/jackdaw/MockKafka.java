package com.obsidiandynamics.jackdaw;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.props.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.zerolog.*;

@Y
public final class MockKafka<K, V> implements Kafka<K, V> {
  private final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
  
  private final int maxPartitions;
  
  private final int maxHistory;
  
  private FallibleMockProducer<K, V> producer;
  
  private final List<FallibleMockConsumer<K, V>> consumers = new ArrayList<>();
  
  private List<ConsumerRecord<K, V>> backlog = new ArrayList<>();
  
  private final Object lock = new Object();
  
  /** Tracks presence of group members. */
  private final Set<String> groups = new HashSet<>();
  
  private ExceptionGenerator<ProducerRecord<K, V>, Exception> sendCallbackExceptionGenerator = ExceptionGenerator.never();
  private ExceptionGenerator<ProducerRecord<K, V>, RuntimeException> sendRuntimeExceptionGenerator = ExceptionGenerator.never();
  private ExceptionGenerator<Map<TopicPartition, OffsetAndMetadata>, Exception> commitExceptionGenerator = ExceptionGenerator.never();
  
  public MockKafka() {
    this(10, 100_000);
  }
  
  public MockKafka(int maxPartitions, int maxHistory) {
    this.maxPartitions = maxPartitions;
    this.maxHistory = maxHistory;
  }
  
  public MockKafka<K, V> withSendCallbackExceptionGenerator(ExceptionGenerator<ProducerRecord<K, V>, Exception> sendCallbackExceptionGenerator) {
    this.sendCallbackExceptionGenerator = sendCallbackExceptionGenerator;
    return this;
  }
  
  public MockKafka<K, V> withSendRuntimeExceptionGenerator(ExceptionGenerator<ProducerRecord<K, V>, RuntimeException> sendRuntimeExceptionGenerator) {
    this.sendRuntimeExceptionGenerator = sendRuntimeExceptionGenerator;
    return this;
  }

  public MockKafka<K, V> withCommitExceptionGenerator(ExceptionGenerator<Map<TopicPartition, OffsetAndMetadata>, Exception> commitExceptionGenerator) {
    this.commitExceptionGenerator = commitExceptionGenerator;
    return this;
  }

  @Override
  public void describeProducer(LogLine logLine, Properties defaults, Properties overrides) {
    logLine.println("Mock producer");
  }
  
  @Override
  public FallibleMockProducer<K, V> getProducer(Properties overrides) {
    return getProducer(new Properties(), overrides);
  }

  @Override
  public FallibleMockProducer<K, V> getProducer(Properties defaults, Properties overrides) {
    final Properties combined = Props.merge(defaults, overrides);
    synchronized (lock) {
      if (producer == null) {
        final String keySerializer = combined.getProperty("key.serializer");
        final String valueSerializer = combined.getProperty("value.serializer");
        producer = new FallibleMockProducer<K, V>(true, instantiate(keySerializer), instantiate(valueSerializer)) {
          {
            this.sendCallbackExceptionGenerator = MockKafka.this.sendCallbackExceptionGenerator;
            this.sendRuntimeExceptionGenerator = MockKafka.this.sendRuntimeExceptionGenerator;
          }
          
          @Override public Future<RecordMetadata> send(ProducerRecord<K, V> r, Callback callback) {
            if (closed.get()) throw new IllegalStateException("Cannot send over a closed producer");
            final RuntimeException generatedRuntime = sendRuntimeExceptionGenerator.inspect(r);
            if (generatedRuntime != null) throw generatedRuntime;
            
            final Exception generatedCallback = sendCallbackExceptionGenerator.inspect(r);
            if (generatedCallback != null) {
              if (callback != null) callback.onCompletion(null, generatedCallback);
              final CompletableFuture<RecordMetadata> f = new CompletableFuture<>();
              f.completeExceptionally(generatedCallback);
              return f;
            } else {
              final Future<RecordMetadata> f = super.send(r, (metadata, exception) -> {
                if (callback != null) callback.onCompletion(metadata, exception);
                final int partition = r.partition() != null ? r.partition() : metadata.partition();
                enqueue(r, partition, metadata.offset());
              });
              return f;
            }
          }
          
          final AtomicBoolean closed = new AtomicBoolean();
          @Override public void close(long timeout, TimeUnit timeUnit) {
            if (closed.compareAndSet(false, true)) {
              super.close();
            }
          }
        };
      }
    }
    return producer;
  }
  
  static final class InvalidPartitionException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;
    InvalidPartitionException(String m) { super(m); }
  }
  
  private void enqueue(ProducerRecord<K, V> r, int partition, long offset) {
    if (partition >= maxPartitions) {
      final String m = String.format("Cannot send message on partition %d, "
          + "a maximum of %d partitions are supported", partition, maxPartitions);
      throw new InvalidPartitionException(m);
    }
    
    final ConsumerRecord<K, V> cr = 
        new ConsumerRecord<>(r.topic(), partition, offset, r.key(), r.value());
    
    final TopicPartition part = new TopicPartition(r.topic(), partition);
    synchronized (lock) {
      backlog.add(cr);
      for (MockConsumer<K, V> consumer : consumers) {
        if (consumer.assignment().contains(part)) {
          consumer.addRecord(cr);
        }
      }
      
      if (producer.history().size() > maxHistory) {
        producer.clear();
        backlog = backlog.subList(backlog.size() - maxHistory, backlog.size());
      }
    }
  }
  
  public List<ConsumerRecord<K, V>> getBacklog() {
    synchronized (lock) {
      return Collections.unmodifiableList(new ArrayList<>(backlog));
    }
  }

  @Override
  public void describeConsumer(LogLine logLine, Properties defaults, Properties overrides) {
    logLine.accept("Mock consumer");
  }
  
  @Override
  public FallibleMockConsumer<K, V> getConsumer(Properties overrides) {
    return getConsumer(new Properties(), overrides);
  }

  @Override
  public FallibleMockConsumer<K, V> getConsumer(Properties defaults, Properties overrides) {
    final Properties combined = Props.merge(defaults, overrides);
    final String groupId = combined.getProperty("group.id");
    final boolean newGroupMember = groupId == null || groups.add(groupId);
    if (newGroupMember) {
      return createAttachedConsumer();
    } else {
      return createDetachedConsumer();
    }
  }
  
  private FallibleMockConsumer<K, V> createDetachedConsumer() {
    return createConsumer(new Object(), new ArrayList<>(1));
  }
  
  private FallibleMockConsumer<K, V> createAttachedConsumer() {
    return createConsumer(lock, consumers);
  }
  
  private FallibleMockConsumer<K, V> createConsumer(Object lock, List<FallibleMockConsumer<K, V>> consumers) {
    final FallibleMockConsumer<K, V> consumer = new FallibleMockConsumer<K, V>(OffsetResetStrategy.EARLIEST) {
      {
        this.commitExceptionGenerator = MockKafka.this.commitExceptionGenerator;
      }
      
      @Override public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        final Exception generated = commitExceptionGenerator.inspect(offsets);
        if (generated != null) {
          if (callback != null) callback.onComplete(offsets, generated);
        } else {
          super.commitAsync(offsets, callback);
        }
      }
      
      @Override public void subscribe(Collection<String> topics) {
        for (String topic : topics) {
          zlg.t("Assigning %s").arg(topic).tag("MockKafka").log();
          synchronized (lock) {
            final List<TopicPartition> partitions = new ArrayList<>(maxPartitions);
            final Map<TopicPartition, Long> offsetRecords = new HashMap<>();
            final List<ConsumerRecord<K, V>> records = new ArrayList<>();
            
            for (int partIdx = 0; partIdx < maxPartitions; partIdx++) {
              final TopicPartition part = new TopicPartition(topic, partIdx);
              partitions.add(part);
              offsetRecords.put(part, 0L);
              
              for (ConsumerRecord<K, V> cr : backlog) {
                if (cr.topic().equals(topic) && cr.partition() == partIdx) {
                  records.add(cr);
                }
              }
            }

            assign(partitions);
            updateBeginningOffsets(offsetRecords);
            for (ConsumerRecord<K, V> cr : records) {
              addRecord(cr);
            }
          }
        }
      }
      
      @Override public List<PartitionInfo> partitionsFor(String topic) {
        final List<PartitionInfo> newInfos = new ArrayList<>(maxPartitions);
        final Map<TopicPartition, Long> offsets = new HashMap<>(maxPartitions);
        
        for (int i = 0; i < maxPartitions; i++) {
          newInfos.add(new PartitionInfo(topic, i, null, new Node[0], new Node[0]));
          offsets.put(new TopicPartition(topic, i), 0L);
        }
        
        synchronized (lock) {
          updateBeginningOffsets(offsets);
          updateEndOffsets(offsets);
        }
        return newInfos;
      }
      
      @Override public ConsumerRecords<K, V> poll(long timeout) {
        // super.poll() disregards the timeout, resulting in a spin loop in the absence of records
        // and resource exhaustion on single-CPU machines
        
        final long endTime = System.currentTimeMillis() + timeout;
        for (;;) {
          final ConsumerRecords<K, V> recs = super.poll(timeout);
          if (! recs.isEmpty()) {
            return recs;
          } else {
            // enforce minimum sleep time if there are no records
            final long remainingMillis = endTime - System.currentTimeMillis();
            if (remainingMillis <= 0) {
              return recs;
            } else {
              try {
                Thread.sleep(Math.min(10, remainingMillis));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return recs;
              }
            }
          }
        }
      }
      
      @Override public void close() {
        synchronized (lock) {
          consumers.remove(this);
        }
        super.close();
      }
    };
    
    synchronized (lock) {
      consumers.add(consumer);
    }
    return consumer;
  }
  
  private static <T> T instantiate(String className) {
    return Exceptions.wrap(() -> Classes.cast(Class.forName(className).getDeclaredConstructor().newInstance()),
                           RuntimeException::new);
  }

  @Override
  public String toString() {
    return MockKafka.class.getSimpleName() + " [maxPartitions: " + maxPartitions + ", maxHistory: " + maxHistory + "]";
  }
}
