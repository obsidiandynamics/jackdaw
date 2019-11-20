package com.obsidiandynamics.jackdaw.sample;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.*;

public final class BarebonesConsumerSample {
  public static void main(String[] args) {
    final Properties consumerProps = new Properties();
    consumerProps.setProperty("bootstrap.servers", "localhost:9092");
    consumerProps.setProperty("key.deserializer", StringDeserializer.class.getName());
    consumerProps.setProperty("value.deserializer", StringDeserializer.class.getName());
    consumerProps.setProperty("group.id", "myGroup");
    consumerProps.setProperty("auto.offset.reset", "earliest");
    consumerProps.setProperty("enable.auto.commit", String.valueOf(false));
    
    try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
      consumer.subscribe(Collections.singleton("test"));
      while (true) {
        final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        for (ConsumerRecord<String, String> record : records) {
          System.out.format("Got record with value %s%n", record.value());
          consumer.commitAsync();
        }
      }
    }
  }
}
