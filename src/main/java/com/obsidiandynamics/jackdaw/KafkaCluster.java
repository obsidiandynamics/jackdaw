package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.func.Functions.*;
import static com.obsidiandynamics.props.PropsFormat.*;

import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.props.*;
import com.obsidiandynamics.yconf.*;

@Y
public final class KafkaCluster<K, V> implements Kafka<K, V> {
  private final KafkaClusterConfig config;

  public KafkaCluster(@YInject(name="clusterConfig") KafkaClusterConfig config) {
    mustExist(config, "Cluster config cannot be null").validate();
    this.config = config;
  }

  public KafkaClusterConfig getConfig() {
    return config;
  }

  private Properties mergeProducerProps(Properties defaults, Properties overrides) {
    mustExist(defaults, "Defaults cannot be null");
    mustExist(overrides, "Overrides cannot be null");
    return Props.merge(defaults, config.getProducerCombinedProps(), overrides);
  }

  @Override
  public KafkaProducer<K, V> getProducer(Properties defaults, Properties overrides) {
    mustExist(defaults, "Defaults cannot be null");
    mustExist(overrides, "Overrides cannot be null");
    return new KafkaProducer<>(mergeProducerProps(defaults, overrides));
  }

  @Override
  public void describeProducer(LogLine logLine, Properties defaults, Properties overrides) {
    mustExist(logLine, "Log line cannot be null");
    mustExist(defaults, "Defaults cannot be null");
    mustExist(overrides, "Overrides cannot be null");
    logLine.println("Producer properties:");
    PropsFormat.printProps(logLine, 
                           mergeProducerProps(defaults, overrides),
                           s -> (overrides.containsKey(s) ? "* " : "- ") + rightPad(25).apply(s),
                           prefix(" "), 
                           any());
  }

  private Properties mergeConsumerProps(Properties defaults, Properties overrides) {
    mustExist(defaults, "Defaults cannot be null");
    mustExist(overrides, "Overrides cannot be null");
    return Props.merge(defaults, config.getConsumerCombinedProps(), overrides);
  }

  @Override
  public KafkaConsumer<K, V> getConsumer(Properties defaults, Properties overrides) {
    mustExist(defaults, "Defaults cannot be null");
    mustExist(overrides, "Overrides cannot be null");
    return new KafkaConsumer<>(mergeConsumerProps(defaults, overrides));
  }

  @Override
  public void describeConsumer(LogLine logLine, Properties defaults, Properties overrides) {
    mustExist(logLine, "Log line cannot be null");
    mustExist(defaults, "Defaults cannot be null");
    mustExist(overrides, "Overrides cannot be null");
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

  @Override
  public KafkaAdminClient getAdminClient() {
    return (KafkaAdminClient) AdminClient.create(config.getCommonProps());
  }
}
