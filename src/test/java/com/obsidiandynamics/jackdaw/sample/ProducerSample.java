package com.obsidiandynamics.jackdaw.sample;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class ProducerSample {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final String BOOTSTRAP_SERVERS = "localhost:9092";
  
  private static final Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers(BOOTSTRAP_SERVERS));
  
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    final Properties props = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .build();
    
    final String topic = "test.plain";
    
    try (AdminClient admin = kafka.getAdminClient()) {
      final Map<String, String> configs = MapBuilder
          .init("segment.bytes", String.valueOf(10_000_000))
          .with("cleanup.policy", "compact")
          .build();
      admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1).configs(configs)));
    }
    
    final int publishIntervalMillis = 100;
    try (Producer<String, String> producer = kafka.getProducer(props)) {
      for (;;) {
        final String value = String.valueOf(System.currentTimeMillis());
        final RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, "test-key", value)).get();
        zlg.i("publishing %s, metadata=%s", z -> z.arg(value).arg(metadata));
        Threads.sleep(publishIntervalMillis);
      }
    }
  }  
}
