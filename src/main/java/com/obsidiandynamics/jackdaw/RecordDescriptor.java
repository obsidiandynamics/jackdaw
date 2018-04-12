package com.obsidiandynamics.jackdaw;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.*;

/**
 *  Generic depiction of a Kafka record, suitable as a substitute for {@link ConsumerRecord} and {@link ProducerRecord}.
 *  
 *  @param <K> Key type.
 *  @param <V> Value type.
 */
final class RecordDescriptor<K, V> {
  private final String topic;
  private final Integer partition;
  private final Headers headers;
  private final K key;
  private final V value;
  private final Long timestamp;
  
  RecordDescriptor(String topic, Integer partition, Headers headers, K key, V value, Long timestamp) {
    this.topic = topic;
    this.partition = partition;
    this.headers = headers;
    this.key = key;
    this.value = value;
    this.timestamp = timestamp;
  }
  
  public String topic() {
    return topic;
  }

  public Integer partition() {
    return partition;
  }

  public Headers headers() {
    return headers;
  }

  public K key() {
    return key;
  }

  public V value() {
    return value;
  }

  public Long timestamp() {
    return timestamp;
  }

  @Override
  public String toString() {
    return RecordDescriptor.class.getSimpleName() + " [topic=" + topic + ", partition=" + partition + ", headers=" + headers + ", key=" + key
           + ", value=" + value + ", timestamp=" + timestamp + "]";
  }
  
  public static <K, V> RecordDescriptor<K, V> forRecord(ProducerRecord<K, V> record) {
    return new RecordDescriptor<>(record.topic(), record.partition(), record.headers(), 
        record.key(), record.value(), record.timestamp());
  }
  
  public static <K, V> RecordDescriptor<K, V> forRecord(ConsumerRecord<K, V> record) {
    return new RecordDescriptor<>(record.topic(), record.partition(), record.headers(), 
        record.key(), record.value(), record.timestamp());
  }
}
