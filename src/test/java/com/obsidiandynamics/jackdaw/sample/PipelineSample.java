package com.obsidiandynamics.jackdaw.sample;

import java.lang.invoke.*;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.jackdaw.AsyncReceiver.*;
import com.obsidiandynamics.yconf.props.*;
import com.obsidiandynamics.zerolog.*;

public final class PipelineSample {
  public static void main(String[] args) throws InterruptedException {
    final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
    final Kafka<String, String> kafka = new KafkaCluster<>(new KafkaClusterConfig().withBootstrapServers("localhost:9092"));
    
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

    final ExceptionHandler exceptionHandler = ExceptionHandler.forPrintStream(System.err);
    final ProducerPipeConfig producerPipeConfig = new ProducerPipeConfig().withAsync(true);
    final ProducerPipe<String, String> producerPipe = new ProducerPipe<>(producerPipeConfig, 
        producer, "ProducerPipeThread", exceptionHandler);
    
    zlg.i("Publishing record");
    // sending doesn't block, not even to serialize the record
    producerPipe.send(new ProducerRecord<>("topic", "key", "value"), null);
    
    final RecordHandler<String, String> recordHandler = records -> {
      zlg.i("Got %d records", z -> z.arg(records::count));
    };
    final ConsumerPipeConfig consumerPipeConfig = new ConsumerPipeConfig().withAsync(true).withBacklogBatches(128);
    final ConsumerPipe<String, String> consumerPipe = new ConsumerPipe<>(consumerPipeConfig, 
        recordHandler, ConsumerPipe.class.getSimpleName());
    
    for (;;) {
      // calling receive() doesn't block (up to the 'backlogBatches' capacity of the underlying queue); 
      // calling poll() blocks as expected
      consumerPipe.receive(consumer.poll(1000));
    }
  }
}
