package com.obsidiandynamics.jackdaw.sample;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

public final class AsyncSample {
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

    // a callback for asynchronously handling records
    final RecordHandler<String, String> recordHandler = records -> {
      zlg.i("Got %d records", z -> z.arg(records::count));
    };

    // a callback for handling exceptions
    final ExceptionHandler exceptionHandler = ExceptionHandler.forPrintStream(System.err);
    
    // wrap the consumer in an AsyncReceiver
    final AsyncReceiver<?, ?> receiver = 
        new AsyncReceiver<>(consumer, 100, "AsyncReceiverThread", recordHandler, exceptionHandler);

    zlg.i("Publishing record");
    producer.send(new ProducerRecord<>("topic", "key", "value"));
    
    // give it some time...
    Threads.sleep(5_000);
    
    // clean up
    producer.close();
    receiver.terminate();
  }
}
