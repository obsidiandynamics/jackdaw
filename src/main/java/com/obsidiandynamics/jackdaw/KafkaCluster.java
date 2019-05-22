package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.jackdaw.KafkaClusterConfig.*;
import static com.obsidiandynamics.props.PropsFormat.*;

import java.util.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.yconf.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.props.*;

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
  public KafkaProducer<K, V> getProducer(Properties defaults, Properties overrides) {
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
  public KafkaConsumer<K, V> getConsumer(Properties defaults, Properties overrides) {
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

  @Override
  public KafkaAdminClient getAdminClient() {
    final String bootstrapServers = config.getCommonProps().getProperty(CONFIG_BOOTSTRAP_SERVERS);
    final Properties props = new PropsBuilder()
        .with(CONFIG_BOOTSTRAP_SERVERS, bootstrapServers)
        .build();
    return (KafkaAdminClient) AdminClient.create(props);
  }
}
