package com.obsidiandynamics.jackdaw.sample;

import static java.util.Collections.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.yconf.props.*;

public final class RunKafkaConsumer {
  public static void main(String[] args) {
    final Properties props = new PropsBuilder()
        .with("bootstrap.servers", "localhost:9092")
        .with("group.id", "sample")
        .with("auto.offset.reset", "earliest")
        .with("enable.auto.commit", String.valueOf(false))
        .with("session.timeout.ms", 6_000)
        .with("heartbeat.interval.ms", 2_000)
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", StringDeserializer.class.getName())
        .build();
    
    final int commitIntervalMillis = 1_000;
    long lastCommitTime = 0;
    Map<TopicPartition, OffsetAndMetadata> offsetToCommit = null;
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Arrays.asList("test"));
      for (;;) {
        System.out.println("polling...");
        final ConsumerRecords<String, String> records = consumer.poll(1_000);
        
        if (! records.isEmpty()) {
          final List<ConsumerRecord<String, String>> recordsList = list(records);
          System.out.format("got %,d records\n", recordsList.size());
          for (ConsumerRecord<String, String> record : recordsList) {
            System.out.format("%,d: %s\n", record.offset(), record.value());
          }
          
          if (System.currentTimeMillis() - lastCommitTime > commitIntervalMillis) {
            if (offsetToCommit != null) {
              System.out.format("Committing %s\n", offsetToCommit);
              consumer.commitAsync(offsetToCommit, null);
              lastCommitTime = System.currentTimeMillis();
            }
            final ConsumerRecord<String, String> lastRecord = recordsList.get(recordsList.size() - 1);
            offsetToCommit = singletonMap(new TopicPartition(lastRecord.topic(), lastRecord.partition()), 
                                          new OffsetAndMetadata(lastRecord.offset()));
          }
        }
      }
    }
  }
  
  private static List<ConsumerRecord<String, String>> list(ConsumerRecords<String, String> records) {
    final List<ConsumerRecord<String, String>> list = new ArrayList<>();
    records.iterator().forEachRemaining(list::add);
    return list;
  }
}
