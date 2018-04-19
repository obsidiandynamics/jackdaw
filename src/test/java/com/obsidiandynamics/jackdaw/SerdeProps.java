package com.obsidiandynamics.jackdaw;

import java.util.*;

import org.apache.kafka.common.serialization.*;

import com.obsidiandynamics.yconf.util.*;

final class SerdeProps {
  enum SerdePair {
    INTEGER (IntegerSerializer.class, IntegerDeserializer.class),
    STRING (StringSerializer.class, StringDeserializer.class);
    
    final Class<? extends Serializer<?>> serializerClass;
    final Class<? extends Deserializer<?>> deserializerClass;
    
    private SerdePair(Class<? extends Serializer<?>> serializerClass, 
                      Class<? extends Deserializer<?>> deserializerClass) {
      this.serializerClass = serializerClass;
      this.deserializerClass = deserializerClass;
    }
  }
  
  private final SerdePair keySerdePair, valueSerdePair;
  
  SerdeProps(SerdePair keyAndValueSerdePair) {
    this(keyAndValueSerdePair, keyAndValueSerdePair);
  }
  
  SerdeProps(SerdePair keySerdePair, SerdePair valueSerdePair) {
    this.keySerdePair = keySerdePair;
    this.valueSerdePair = valueSerdePair;
  }
  
  Properties producer() {
    return new PropsBuilder()
        .with("key.serializer", keySerdePair.serializerClass.getName())
        .with("value.serializer", valueSerdePair.serializerClass.getName())
        .build();
  }
  
  Properties consumer() {
    return new PropsBuilder()
        .with("key.deserializer", keySerdePair.serializerClass.getName())
        .with("value.deserializer", valueSerdePair.serializerClass.getName())
        .build();
  }
}