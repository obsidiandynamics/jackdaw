package com.obsidiandynamics.jackdaw.sample;

import java.time.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class SyncSample {
  public static void main(String[] args) {
    final Zlg zlg = Zlg.forDeclaringClass().get();
    final Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig()
                                                           .withBootstrapServers("localhost:9092"));
    
    final Properties producerProps = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .build();
    final Producer<String, String> producer = kafka.getProducer(producerProps);
    
    final Properties consumerProps = new PropsBuilder()
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", StringDeserializer.class.getName())
        .with("group.id", "group")
        .with("auto.offset.reset", "earliest")
        .with("enable.auto.commit", true)
        .build();
    final Consumer<String, String> consumer = kafka.getConsumer(consumerProps);
    consumer.subscribe(Collections.singleton("topic"));

    zlg.i("Publishing record");
    producer.send(new ProducerRecord<>("topic", "key", "value"));
    
    for (;;) {
      final ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
      zlg.i("Got %d record(s)", z -> z.arg(records::count));
    }
  }
}
