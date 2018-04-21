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

public final class MockAsyncSample {
  public static void main(String[] args) {
    final Zlg zlg = Zlg.forDeclaringClass().get();
    final Kafka<String, String> kafka = new MockKafka<>();
    
    final Properties producerProps = new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .build();
    final Producer<String, String> producer = kafka.getProducer(producerProps);
    
    final Properties consumerProps = new PropsBuilder()
        .with("group.id", "group")
        .build();
    
    final Consumer<String, String> consumer = kafka.getConsumer(consumerProps);
    consumer.subscribe(Collections.singleton("topic"));
    
    final RecordHandler<String, String> recordHandler = records -> {
      zlg.i("Got %d record(s)", z -> z.arg(records::count));
    };
    
    final ExceptionHandler exceptionHandler = ExceptionHandler.forPrintStream(System.err);
    final AsyncReceiver<String, String> receiver = 
        new AsyncReceiver<>(consumer, 100, "AsyncReceiverThread", recordHandler, exceptionHandler);

    zlg.i("Publishing record");
    producer.send(new ProducerRecord<>("topic", "key", "value"));
    
    Threads.sleep(10_000);
    
    producer.close();
    receiver.terminate();
  }
}
