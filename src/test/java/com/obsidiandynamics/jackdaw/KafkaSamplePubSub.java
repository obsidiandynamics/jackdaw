package com.obsidiandynamics.jackdaw;

import java.lang.invoke.*;
import java.text.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.props.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaSamplePubSub {
  private static final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
  
  private static final boolean MOCK = false;
  private static final String BROKERS = "localhost:9092";
  private static final String TOPIC = "test";
  private static final String CONSUMER_GROUP = "test";
  private static final long PUBLISH_INTERVAL = 100;
  private static final Kafka<String, String> KAFKA = MOCK 
      ? new MockKafka<>() 
      : new KafkaCluster<>(new KafkaClusterConfig()
          .withCommonProps(new PropsBuilder().with("bootstrap.servers", BROKERS).build()));
  
  private static final class SamplePublisher extends Thread {
    private static Properties getProps() {
      final Properties props = new Properties();
      props.setProperty("key.serializer", StringSerializer.class.getName());
      props.setProperty("value.serializer", StringSerializer.class.getName());
      return props;
    }
    
    private final Producer<String, String> producer;
    private volatile boolean running = true;
    
    SamplePublisher() {
      super("Kafka-SamplePublisher");
      producer = KAFKA.getProducer(getProps());
      start();
    }
    
    @Override public void run() {
      while (running) {
        send();
        if (PUBLISH_INTERVAL != 0) Threads.sleep(PUBLISH_INTERVAL);
      }
      producer.close();
    }
    
    private void send() {
      final long now = System.currentTimeMillis();
      final String msg = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").format(new Date(now));
      final ProducerRecord<String, String> rec = new ProducerRecord<>(TOPIC, String.valueOf(now), msg);
      producer.send(rec, (metadata, exception) -> {
        zlg.i("tx [%s], key: %s, value: %s").arg(metadata).arg(rec::key).arg(rec::value).tag("p").log();
      });
    }
    
    void close() {
      running = false;
    }
  }
  
  private static final class SampleSubscriber {
    private static Properties getProps() {
      final Properties props = new Properties();
      props.setProperty("group.id", CONSUMER_GROUP);
      props.setProperty("key.deserializer", StringDeserializer.class.getName());
      props.setProperty("value.deserializer", StringDeserializer.class.getName());
      return props;
    }

    private final KafkaReceiver<String, String> receiver;
    private final Consumer<String, String> consumer;
    
    SampleSubscriber() {
      consumer = KAFKA.getConsumer(getProps());
      consumer.subscribe(Arrays.asList(TOPIC));
      receiver = new KafkaReceiver<>(consumer, 100, "Kafka-SampleSubscriber", this::onReceive, this::onError);
    }
    
    private void onReceive(ConsumerRecords<String, String> records) {
      for (ConsumerRecord<String, String> record : records) {
        zlg.i("rx [%s], key: %s, value: %s")
        .arg(record, SampleSubscriber::formatMetadata).arg(record::key).arg(record::value)
        .tag("c").log();
      }
    }
    
    private static String formatMetadata(ConsumerRecord<?, ?> rec) {
      return String.format("%s-%d@%d", rec.topic(), rec.partition(), rec.offset());
    }
    
    
    private void onError(Throwable cause) {
      zlg.e("exception: %s").arg(cause).tag("c").log();
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
