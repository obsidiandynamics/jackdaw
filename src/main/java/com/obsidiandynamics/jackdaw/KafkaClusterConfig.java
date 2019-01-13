package com.obsidiandynamics.jackdaw;

import java.util.*;

import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.yconf.util.*;

@Y(KafkaClusterConfig.Mapper.class)
public final class KafkaClusterConfig {
  public static final class Mapper implements TypeMapper {
    @Override public Object map(YObject y, Class<?> type) {
      return new KafkaClusterConfig()
          .withCommonProps(y.mapAttribute("common", PropsBuilder.class).build())
          .withProducerProps(y.mapAttribute("producer", PropsBuilder.class).build())
          .withConsumerProps(y.mapAttribute("consumer", PropsBuilder.class).build());
    }
  }
  
  public static final String CONFIG_BOOTSTRAP_SERVERS = "bootstrap.servers";
  
  private Properties common = new PropsBuilder()
      .withSystemDefault(CONFIG_BOOTSTRAP_SERVERS, null)
      .build();

  private Properties producer = new Properties();
  
  private Properties consumer = new Properties();
  
  public void validate() {
    if (common.getProperty(CONFIG_BOOTSTRAP_SERVERS) == null) {
      throw new IllegalArgumentException("Must specify a value for '" + CONFIG_BOOTSTRAP_SERVERS + "'");
    }
  }
  
  public KafkaClusterConfig withBootstrapServers(String bootstrapServers) {
    return withCommonProps(Collections.singletonMap(CONFIG_BOOTSTRAP_SERVERS, bootstrapServers));
  }
  
  public Properties getCommonProps() {
    return common;
  }
  
  private Properties getCommonPropsCopy() {
    final Properties props = new Properties();
    props.putAll(common);
    return props;
  }
  
  public void setCommonProps(Map<Object, Object> common) {
    this.common.putAll(common);
  }
  
  public KafkaClusterConfig withCommonProps(Map<Object, Object> common) {
    setCommonProps(common);
    return this;
  }

  public Properties getProducerCombinedProps() {
    final Properties props = getCommonPropsCopy();
    props.putAll(producer);
    return props;
  }
  
  public void setProducerProps(Map<Object, Object> producer) {
    this.producer.putAll(producer);
  }
  
  public KafkaClusterConfig withProducerProps(Map<Object, Object> producer) {
    setProducerProps(producer);
    return this;
  }
  
  public Properties getConsumerCombinedProps() {
    final Properties props = getCommonPropsCopy();
    props.putAll(consumer);
    return props;
  }
  
  public void setConsumerProps(Map<Object, Object> consumer) {
    this.consumer.putAll(consumer);
  }
  
  public KafkaClusterConfig withConsumerProps(Map<Object, Object> consumer) {
    setConsumerProps(consumer);
    return this;
  }

  @Override
  public String toString() {
    return KafkaClusterConfig.class.getSimpleName() + " [common: " + common + ", producer: " + 
        producer + ", consumer: " + consumer + "]";
  }
}
