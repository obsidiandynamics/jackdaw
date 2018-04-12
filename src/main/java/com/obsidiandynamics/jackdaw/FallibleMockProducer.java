package com.obsidiandynamics.jackdaw;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.*;

public abstract class FallibleMockProducer<K, V> extends MockProducer<K, V> {
  protected ExceptionGenerator<ProducerRecord<K, V>, Exception> sendCallbackExceptionGenerator = ExceptionGenerator.never();
  protected ExceptionGenerator<ProducerRecord<K, V>, RuntimeException> sendRuntimeExceptionGenerator = ExceptionGenerator.never();
  
  FallibleMockProducer(boolean autoComplete,
                       Serializer<K> keySerializer,
                       Serializer<V> valueSerializer) {
    super(autoComplete, keySerializer, valueSerializer);
  }
  
  public void setSendCallbackExceptionGenerator(ExceptionGenerator<ProducerRecord<K, V>, Exception> sendCallbackExceptionGenerator) {
    this.sendCallbackExceptionGenerator = sendCallbackExceptionGenerator;
  }
  
  public void setSendRuntimeExceptionGenerator(ExceptionGenerator<ProducerRecord<K, V>, RuntimeException> sendRuntimeExceptionGenerator) {
    this.sendRuntimeExceptionGenerator = sendRuntimeExceptionGenerator;
  }
}
