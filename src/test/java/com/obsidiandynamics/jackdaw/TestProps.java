package com.obsidiandynamics.jackdaw;

import java.util.*;

import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.yconf.props.*;

final class TestProps {
  enum SerdePair {
    INTEGER (IntegerSerializer.class, IntegerDeserializer.class),
    STRING (StringSerializer.class, StringDeserializer.class);
    
    private final Class<? extends Serializer<?>> serClass;
    private final Class<? extends Deserializer<?>> desClass;
    
    private SerdePair(Class<? extends Serializer<?>> serClass, Class<? extends Deserializer<?>> desClass) {
      this.serClass = serClass;
      this.desClass = desClass;
    }
  }
  
  private TestProps() {}
  
  static Properties producer() {
    return new PropsBuilder()
        .with("key.serializer", StringSerializer.class.getName())
        .with("value.serializer", StringSerializer.class.getName())
        .build();
  }
  
  static Properties consumer() {
    return new PropsBuilder()
        .with("key.deserializer", StringDeserializer.class.getName())
        .with("value.deserializer", StringDeserializer.class.getName())
        .build();
  }
}