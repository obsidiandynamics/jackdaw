package com.obsidiandynamics.jackdaw.sample;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class ConsumerSample {
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
    final int commitIntervalMillis = 1_000;
    long lastCommitTime = 0;
    final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    try (Consumer<String, String> consumer = kafka.getConsumer(props)) {
      consumer.subscribe(Arrays.asList("test"));
      for (;;) {
        zlg.i("polling...");
        final ConsumerRecords<String, String> records = consumer.poll(100);
        
        if (! records.isEmpty()) {
          final List<ConsumerRecord<String, String>> recordsList = list(records);
          zlg.i("consumed %,d record(s)", z -> z.arg(recordsList::size));
          for (ConsumerRecord<String, String> record : recordsList) {
            zlg.i("  %,d: %s", z -> z.arg(record::offset).arg(record::value));
          }
        }
        
        if (System.currentTimeMillis() - lastCommitTime > commitIntervalMillis) {
          if (! offsetsToCommit.isEmpty()) {
            zlg.i("committing %s", z -> z.arg(offsetsToCommit));
            consumer.commitAsync(offsetsToCommit, null);
            offsetsToCommit.clear();
            lastCommitTime = System.currentTimeMillis();
          }
        }
        
        if (! records.isEmpty()) {
          final List<ConsumerRecord<String, String>> recordsList = list(records);
          final ConsumerRecord<String, String> lastRecord = recordsList.get(recordsList.size() - 1);
          offsetsToCommit.put(new TopicPartition(lastRecord.topic(), lastRecord.partition()), 
                              new OffsetAndMetadata(lastRecord.offset()));
        }
        
        Threads.sleep(pollIntervalMillis);
      }
    }
  }
  
  private static List<ConsumerRecord<String, String>> list(ConsumerRecords<String, String> records) {
    final List<ConsumerRecord<String, String>> list = new ArrayList<>();
    records.iterator().forEachRemaining(list::add);
    return list;
  }
}
