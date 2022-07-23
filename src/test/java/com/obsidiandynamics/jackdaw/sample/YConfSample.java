package com.obsidiandynamics.jackdaw.sample;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.jackdaw.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.yconf.util.*;

public class YConfSample {
  public static void main(String[] args) throws IOException {
    final Kafka<?, ?> kafka = new MappingContext()
        .withParser(new SnakeyamlParser())
        .fromStream(Files.newInputStream(Paths.get("src/test/resources/kafka-cluster.conf")))
        .map(Kafka.class);
    
    // default properties
    final Properties defaults = new PropsBuilder()
        .with("compression.type", "lz4")
        .build();
    
    // override the properties from the config
    final Properties overrides = new PropsBuilder()
        .with("max.in.flight.requests.per.connection", 1)
        .build();
    
    final Producer<?, ?> producer = kafka.getProducer(defaults, overrides);
    // do stuff with the producer
    // ...
    producer.close();
  }
}
