package com.obsidiandynamics.jackdaw.sample;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class ThreadedConsumerSample {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  
  private static Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers(BOOTSTRAP_SERVERS));
  
  public static void main(String[] args) {
    final Properties props = new PropsBuilder()
        .with("group.id", "sample")
        .with("auto.offset.reset", "earliest")
        .with("enable.auto.commit", String.valueOf(false))
        .with("session.timeout.ms", 6_000)
        .with("heartbeat.interval.ms", 2_000)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", StringDeserializer.class.getName())
        .build();
    
    final int pollIntervalMillis = 100;
    final boolean commitAsync = false;
    final int parallelism = 8;
    final int processingTimeMillis = 100;
    
    final Map<TopicPartition, OffsetAndMetadata> readOffsets = new HashMap<>();
    final Map<TopicPartition, OffsetAndMetadata> confirmedOffsets = new HashMap<>();
    
    final ExecutorService executor = Executors.newWorkStealingPool(parallelism);
    final Object offsetsLock = new Object();
    try (Consumer<String, String> consumer = kafka.getConsumer(props)) {
      final ConsumerRebalanceListener rebalanceListener = new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          zlg.i("listener: revoked %s (current assignment: %s)", z -> z.arg(partitions).arg(consumer::assignment));
          if (! partitions.isEmpty()) {
            zlg.i("waiting for offsets to be committed...");
            for (;;) {
              final Map<TopicPartition, OffsetAndMetadata> lastConfirmedOffsets;
              synchronized (offsetsLock) {
                if (! confirmedOffsets.isEmpty()) {
                  final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = offsetsToCommit(confirmedOffsets);
                  zlg.i("committing %s", z -> z.arg(offsetsToCommit));
                  if (commitAsync) {
                    consumer.commitAsync(offsetsToCommit, null);
                  } else {
                    consumer.commitSync(offsetsToCommit, Duration.ofMillis(1_000));
                  }
                  lastConfirmedOffsets = new HashMap<>(confirmedOffsets);
                  confirmedOffsets.clear();
                } else {
                  lastConfirmedOffsets = Collections.emptyMap();
                }
              }
              
              if (lastConfirmedOffsets.equals(readOffsets)) {
                break;
              } else {
                zlg.i("expected: %s, got: %s", z -> z.arg(readOffsets).arg(lastConfirmedOffsets));
              }
              Threads.sleep(10);
            }
            zlg.i("all offsets committed, yielding partition assignments");
          }
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          zlg.i("listener: assigned %s", z -> z.arg(partitions));
        }
      };
      consumer.subscribe(Arrays.asList("test"), rebalanceListener);
      for (;;) {
        zlg.i("polling...");
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        if (! records.isEmpty()) {
          final List<ConsumerRecord<String, String>> recordsList = list(records);
          synchronized (offsetsLock) {
            for (ConsumerRecord<String, String> record : recordsList) {
              final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
              readOffsets.put(topicPartition, new OffsetAndMetadata(record.offset()));
            }
          }
          
          zlg.i("dispatching %,d record(s)", z -> z.arg(recordsList::size));
          
          // dispatch records in parallel; commit offsets for each when done
          for (ConsumerRecord<String, String> record : recordsList) {
            executor.submit(() -> {
              zlg.i("  %,d: %s", z -> z.arg(record::offset).arg(record::value));
              Threads.sleep(processingTimeMillis);
              
              final TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
              // unsafe: we commit the highest offset, which also ends up committing all prior records, which
              // might still be backlogged
              synchronized (offsetsLock) {
                final OffsetAndMetadata latestQueued = confirmedOffsets.get(topicPartition);
                if (latestQueued == null || latestQueued.offset() < record.offset()) {
                  confirmedOffsets.put(topicPartition, new OffsetAndMetadata(record.offset()));
                }
              }
            });
          }
        }
        
        Threads.sleep(pollIntervalMillis);
      }
    }
  }
  
  private static Map<TopicPartition, OffsetAndMetadata> offsetsToCommit(Map<TopicPartition, OffsetAndMetadata> confirmedOffsets) {
    final Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>(confirmedOffsets.size(), 1f);
    for (Map.Entry<TopicPartition, OffsetAndMetadata> confirmedEntry : confirmedOffsets.entrySet()) {
      commitOffsets.put(confirmedEntry.getKey(), new OffsetAndMetadata(confirmedEntry.getValue().offset() + 1));
    }
    return commitOffsets;
  }
  
  private static List<ConsumerRecord<String, String>> list(ConsumerRecords<String, String> records) {
    final List<ConsumerRecord<String, String>> list = new ArrayList<>();
    records.iterator().forEachRemaining(list::add);
    return list;
  }
}
