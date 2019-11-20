package com.obsidiandynamics.jackdaw.sample;

import java.util.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public final class BarebonesProducerSample {
  public static void main(String[] args) throws InterruptedException {
    final Properties producerProps = new Properties();
    producerProps.setProperty("bootstrap.servers", "localhost:9092");
    producerProps.setProperty("key.serializer", StringSerializer.class.getName());
    producerProps.setProperty("value.serializer", StringSerializer.class.getName());
    
    try (Producer<String, String> producer = new KafkaProducer<>(producerProps)) {
      while (true) {
        final String value = new Date().toString();
        System.out.format("Publishing record with value %s%n", value);
        producer.send(new ProducerRecord<>("test", "myKey", value));
        Thread.sleep(1000);
      }
    }
  }
}
