package com.obsidiandynamics.jackdaw.sample;

import java.text.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

/**
 *  Demonstrates pub-sub with either {@link KafkaCluster} or {@link MockKafka}.<p>
 *  
 *  The subscriber is asynchronous, driven by a {@link AsyncReceiver}.
 */
public final class PubSubSample {
  private static final Zlg zlg = Zlg.forDeclaringClass().get();
  
  private static final boolean MOCK = false;
  private static final String BROKERS = "localhost:9092";
  private static final String TOPIC = "test";
  private static final String CONSUMER_GROUP = "test";
  private static final long PUBLISH_INTERVAL_MILLIS = 100;
  
  private static final Kafka<String, String> kafka = MOCK 
      ? new MockKafka<>() 
      : new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers(BROKERS));
  
  private static final class SamplePublisher extends Thread {
    private static Properties getProps() {
      return new PropsBuilder()
          .with("key.serializer", StringSerializer.class.getName())
          .with("value.serializer", StringSerializer.class.getName())
          .build();
    }
    
    private final Producer<String, String> producer;
    private volatile boolean running = true;
    
    SamplePublisher() {
      super(SamplePublisher.class.getSimpleName());
      producer = kafka.getProducer(getProps());
      start();
    }
    
    @Override public void run() {
      while (running) {
        send();
        Threads.sleep(PUBLISH_INTERVAL_MILLIS);
      }
      producer.close();
    }
    
    private void send() {
      final long now = System.currentTimeMillis();
      final String msg = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date(now));
      final ProducerRecord<String, String> rec = new ProducerRecord<>(TOPIC, String.valueOf(now), msg);
      producer.send(rec, (metadata, exception) -> {
        zlg.i("[%s], key: %s, value: %s", z -> z.arg(metadata).arg(rec::key).arg(rec::value).tag("tx"));
      });
    }
    
    void close() {
      running = false;
    }
  }
  
  private static final class SampleSubscriber {
    private static Properties getProps() {
      return new PropsBuilder()
          .with("group.id", CONSUMER_GROUP)
          .with("key.deserializer", StringDeserializer.class.getName())
          .with("value.deserializer", StringDeserializer.class.getName())
          .build();
    }

    private final AsyncReceiver<String, String> receiver;
    private final Consumer<String, String> consumer;
    
    SampleSubscriber() {
      consumer = kafka.getConsumer(getProps());
      consumer.subscribe(Arrays.asList(TOPIC));
      receiver = new AsyncReceiver<>(consumer, 100, SampleSubscriber.class.getSimpleName(), this::onReceive, this::onError);
    }
    
    private void onReceive(ConsumerRecords<String, String> records) {
      for (ConsumerRecord<String, String> record : records) {
        zlg.i("[%s], key: %s, value: %s", 
              z -> z
              .arg(Args.map(Args.ref(record), SampleSubscriber::formatMetadata))
              .arg(record::key)
              .arg(record::value)
              .tag("rx"));
      }
    }
    
    private static String formatMetadata(ConsumerRecord<?, ?> rec) {
      return String.format("%s-%d@%d", rec.topic(), rec.partition(), rec.offset());
    }
    
    private void onError(String message, Throwable cause) {
      zlg.e("%s: %s", z -> z.arg(message).arg(cause).tag("rx"));
    }
    
    void close() {
      receiver.terminate();
    }
  }
  
  public static void main(String[] args) {
    final SamplePublisher pub = new SamplePublisher();
    Threads.sleep(500);
    final SampleSubscriber sub = new SampleSubscriber();
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      zlg.i("Shutting down");
      try {
        pub.close();
        sub.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }, "ShutdownHook"));
    Threads.sleep(Long.MAX_VALUE);
  }
}
