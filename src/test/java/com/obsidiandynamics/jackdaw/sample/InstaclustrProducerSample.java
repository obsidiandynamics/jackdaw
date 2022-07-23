package com.obsidiandynamics.jackdaw.sample;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class InstaclustrProducerSample {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final String BOOTSTRAP_SERVERS = "x.x.x.x:9092";
  
  private static final Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig()
      .withBootstrapServers(BOOTSTRAP_SERVERS)
      .withCommonProps(new PropsBuilder()
                       .with("security.protocol", "SASL_SSL")
                       .with("ssl.endpoint.identification.algorithm", "https")
                       .with("ssl.truststore.location", "truststore.jks")
                       .with("ssl.truststore.password", "instaclustr")
                       .with("sasl.mechanism", "SCRAM-SHA-256")
                       .with("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required\n"
                           + "username=\"xxx\"\n"
                           + "password=\"xxx\";")
                       .build()));
  
  public static void main(String[] args) throws InterruptedException, ExecutionException {
    final Properties props = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .with("acks", "all")
        .with("max.in.flight.requests.per.connection", 1)
        .with("retries", Integer.MAX_VALUE)
        .build();
    
    final String topic = "sandbox.emil.test";
    
    try (AdminClient admin = kafka.getAdminClient()) {
      admin.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1)));
    }

    final int publishIntervalMillis = 100;
    try (Producer<String, String> producer = kafka.getProducer(props)) {
      for (;;) {
        final String value = String.valueOf(System.currentTimeMillis());
        final RecordMetadata metadata = producer.send(new ProducerRecord<>(topic, value)).get();
        zlg.i("publishing %s, metadata=%s", z -> z.arg(value).arg(metadata));
        Threads.sleep(publishIntervalMillis);
      }
    }
  }  
}
