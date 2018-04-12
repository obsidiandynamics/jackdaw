package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.yconf.props.PropsFormat.*;

import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.yconf.props.*;

@Y
public final class KafkaCluster<K, V> implements Kafka<K, V> {
  private final KafkaClusterConfig config;

  public KafkaCluster(@YInject(name="clusterConfig") KafkaClusterConfig config) {
    config.validate();
    this.config = config;
  }

  public KafkaClusterConfig getConfig() {
    return config;
  }

  private Properties mergeProducerProps(Properties defaults, Properties overrides) {
    return Props.merge(defaults, config.getProducerCombinedProps(), overrides);
  }

  @Override
  public Producer<K, V> getProducer(Properties defaults, Properties overrides) {
    return new KafkaProducer<>(mergeProducerProps(defaults, overrides));
  }

  @Override
  public void describeProducer(LogLine logLine, Properties defaults, Properties overrides) {
    logLine.println("Producer properties:");
    PropsFormat.printProps(logLine, 
                           mergeProducerProps(defaults, overrides),
                           s -> (overrides.containsKey(s) ? "* " : "- ") + rightPad(25).apply(s),
                           prefix(" "), 
                           any());
  }

  private Properties mergeConsumerProps(Properties defaults, Properties overrides) {
    return Props.merge(defaults, config.getConsumerCombinedProps(), overrides);
  }

  @Override
  public Consumer<K, V> getConsumer(Properties defaults, Properties overrides) {
    return new KafkaConsumer<>(mergeConsumerProps(defaults, overrides));
  }

  @Override
  public void describeConsumer(LogLine logLine, Properties defaults, Properties overrides) {
    logLine.println("Consumer properties:");
    PropsFormat.printProps(logLine, 
                           mergeConsumerProps(defaults, overrides),
                           s -> (overrides.containsKey(s) ? "* " : "- ") + rightPad(25).apply(s),
                           prefix(" "), 
                           any());
  }

  @Override
  public String toString() {
    return KafkaCluster.class.getSimpleName() + " [config: " + config + "]";
  }
}
