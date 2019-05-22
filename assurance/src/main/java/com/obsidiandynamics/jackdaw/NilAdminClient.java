package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.func.Classes.*;
import static java.util.Collections.*;
import static java.util.function.Function.*;

import java.time.*;
import java.util.*;
import java.util.function.*;
import java.util.stream.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.XDeleteAclsResult.*;
import org.apache.kafka.clients.admin.XDescribeReplicaLogDirsResult.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.*;
import org.apache.kafka.common.internals.*;
import org.apache.kafka.common.requests.*;

public final class NilAdminClient extends AdminClient {
  private static final NilAdminClient INSTANCE = new NilAdminClient();
  
  public static NilAdminClient getInstance() { return INSTANCE; }
  
  private NilAdminClient() {}
  
  @Override
  public void close(Duration timeout) {}
  
  private static <T, K> Map<K, KafkaFuture<Void>> complete(Collection<T> inputs, 
                                                           Function<? super T, ? extends K> keyExtractor) {
    return complete(inputs, keyExtractor, __ -> null);
  }
  
  private static <T, K, V> Map<K, KafkaFuture<V>> complete(Collection<T> inputs, 
                                                           Function<? super T, ? extends K> keyExtractor, 
                                                           Function<? super T, ? extends V> valueGenerator) {
    final Map<K, KafkaFuture<V>> futures = new HashMap<>();
    for (T input : inputs) {
      futures.put(keyExtractor.apply(input), complete(valueGenerator.apply(input)));
    }
    return futures;
  }
  
  private static <T> KafkaFuture<T> completeNull() {
    return complete(null);
  }
  
  private static <T> KafkaFuture<T> complete(T value) {
    return KafkaFuture.completedFuture(value);
  }

  @Override
  public CreateTopicsResult createTopics(Collection<NewTopic> newTopics, CreateTopicsOptions options) {
    return new XCreateTopicsResult(complete(newTopics, NewTopic::name));
  }

  @Override
  public DeleteTopicsResult deleteTopics(Collection<String> topics, DeleteTopicsOptions options) {
    return new XDeleteTopicsResult(complete(topics, identity()));
  }

  @Override
  public ListTopicsResult listTopics(ListTopicsOptions options) {
    return new XListTopicsResult(KafkaFuture.completedFuture(emptyMap()));
  }

  @Override
  public DescribeTopicsResult describeTopics(Collection<String> topicNames, DescribeTopicsOptions options) {
    return new XDescribeTopicsResult(complete(topicNames, identity(), name -> new TopicDescription(name, false, emptyList())));
  }

  @Override
  public DescribeClusterResult describeCluster(DescribeClusterOptions options) {
    return new XDescribeClusterResult(completeNull(), completeNull(), completeNull());
  }

  @Override
  public DescribeAclsResult describeAcls(AclBindingFilter filter, DescribeAclsOptions options) {
    return new XDescribeAclsResult(complete(emptySet()));
  }

  @Override
  public CreateAclsResult createAcls(Collection<AclBinding> acls, CreateAclsOptions options) {
    return new XCreateAclsResult(complete(acls, identity(), __ -> null));
  }

  @Override
  public DeleteAclsResult deleteAcls(Collection<AclBindingFilter> filters, DeleteAclsOptions options) {
    return new XDeleteAclsResult(complete(filters, identity(), __ -> new XFilterResults(emptyList())));
  }

  @Override
  public DescribeConfigsResult describeConfigs(Collection<ConfigResource> resources, DescribeConfigsOptions options) {
    return new XDescribeConfigsResult(complete(resources, identity(), __ -> new Config(emptySet())));
  }

  @Override
  public AlterConfigsResult alterConfigs(Map<ConfigResource, Config> configs, AlterConfigsOptions options) {
    return new XAlterConfigsResult(complete(configs.keySet(), identity(), __ -> null));
  }

  @Override
  public AlterReplicaLogDirsResult alterReplicaLogDirs(Map<TopicPartitionReplica, String> replicaAssignment,
                                                       AlterReplicaLogDirsOptions options) {
    return new XAlterReplicaLogDirsResult(complete(replicaAssignment.keySet(), identity(), __ -> null));
  }

  @Override
  public DescribeLogDirsResult describeLogDirs(Collection<Integer> brokers, DescribeLogDirsOptions options) {
    return new XDescribeLogDirsResult(complete(brokers, identity(), __ -> emptyMap()));
  }

  @Override
  public DescribeReplicaLogDirsResult describeReplicaLogDirs(Collection<TopicPartitionReplica> replicas,
                                                             DescribeReplicaLogDirsOptions options) {
    return new XDescribeReplicaLogDirsResult(complete(replicas, identity(), __ -> new XReplicaLogDirInfo("", 0, "", 0)));
  }

  @Override
  public CreatePartitionsResult createPartitions(Map<String, NewPartitions> newPartitions,
                                                 CreatePartitionsOptions options) {
    return new XCreatePartitionsResult(complete(newPartitions.keySet(), identity()));
  }

  @Override
  public DeleteRecordsResult deleteRecords(Map<TopicPartition, RecordsToDelete> recordsToDelete,
                                           DeleteRecordsOptions options) {
    return new XDeleteRecordsResult(complete(recordsToDelete.keySet(), identity(), __ -> new DeletedRecords(0)));
  }

  @Override
  public CreateDelegationTokenResult createDelegationToken(CreateDelegationTokenOptions options) {
    return new XCreateDelegationTokenResult(completeNull());
  }

  @Override
  public RenewDelegationTokenResult renewDelegationToken(byte[] hmac, RenewDelegationTokenOptions options) {
    return new XRenewDelegationTokenResult(complete(0L));
  }

  @Override
  public ExpireDelegationTokenResult expireDelegationToken(byte[] hmac, ExpireDelegationTokenOptions options) {
    return new XExpireDelegationTokenResult(complete(0L));
  }

  @Override
  public DescribeDelegationTokenResult describeDelegationToken(DescribeDelegationTokenOptions options) {
    return new XDescribeDelegationTokenResult(complete(emptyList()));
  }

  @Override
  public DescribeConsumerGroupsResult describeConsumerGroups(Collection<String> groupIds,
                                                             DescribeConsumerGroupsOptions options) {
    return new XDescribeConsumerGroupsResult(complete(groupIds, 
                                                      identity(), 
                                                      groupId -> new ConsumerGroupDescription(groupId, true, emptySet(), "", ConsumerGroupState.UNKNOWN, null)));
  }

  @Override
  public ListConsumerGroupsResult listConsumerGroups(ListConsumerGroupsOptions options) {
    return new XListConsumerGroupsResult(cast(KafkaFutureImpl.completedFuture(emptyList())));
  }

  @Override
  public ListConsumerGroupOffsetsResult listConsumerGroupOffsets(String groupId,
                                                                 ListConsumerGroupOffsetsOptions options) {
    return new XListConsumerGroupOffsetsResult(complete(emptyMap()));
  }

  @Override
  public DeleteConsumerGroupsResult deleteConsumerGroups(Collection<String> groupIds,
                                                         DeleteConsumerGroupsOptions options) {
    return new XDeleteConsumerGroupsResult(complete(groupIds, identity()));
  }

  @Override
  public ElectPreferredLeadersResult electPreferredLeaders(Collection<TopicPartition> partitions,
                                                           ElectPreferredLeadersOptions options) {
    final Map<TopicPartition, ApiError> map = partitions.stream()
        .collect(Collectors.toMap(identity(), __ -> ApiError.NONE));
    final KafkaFutureImpl<Map<TopicPartition, ApiError>> electionFuture = cast(complete(map));
    return new XElectPreferredLeadersResult(electionFuture, new HashSet<>(partitions));
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return emptyMap();
  }
}
