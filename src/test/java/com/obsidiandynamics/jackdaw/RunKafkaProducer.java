package com.obsidiandynamics.jackdaw;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.yconf.props.*;

public final class RunKafkaProducer {
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    final Properties props = new PropsBuilder()
        .with("bootstrap.servers", "localhost:9092")
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .build();
    
    final String topic = "test";
    final int publishIntervalMillis = 100;
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
      for (;;) {
        final String value = String.valueOf(System.currentTimeMillis());
        final RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, value)).get();
        System.out.format("publishing %s, metadata=%s\n", value, metadata);
        Thread.sleep(publishIntervalMillis);
      }
    }
  }  
}
