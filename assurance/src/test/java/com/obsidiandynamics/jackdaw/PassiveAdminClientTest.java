package com.obsidiandynamics.jackdaw;

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.resource.*;
import org.assertj.core.api.*;
import org.junit.*;

public final class PassiveAdminClientTest {
  private PassiveAdminClient adminClient;
  
  @Before
  public void before() {
    adminClient = PassiveAdminClient.getInstance();
  }
  
  @Test
  public void testSingletion() {
    assertSame(adminClient, PassiveAdminClient.getInstance());
  }
  
  @Test
  public void testClose() {
    adminClient.close();
  }
  
  @Test
  public void testListConsumerGroupOffsets() throws InterruptedException, ExecutionException {
    assertEquals(emptyMap(), adminClient.listConsumerGroupOffsets("testGroup").partitionsToOffsetAndMetadata().get());
  }

  @Test
  public void testCreateTopics() throws Exception {
    assertEquals(emptyMap(), adminClient.createTopics(emptySet()).values());
  }

  @Test
  public void testDeleteTopics() throws Exception {
    assertEquals(emptyMap(), adminClient.deleteTopics(emptySet()).values());
  }

  @Test
  public void testListTopics() throws Exception {
    assertEquals(emptySet(), adminClient.listTopics().listings().get());
  }

  @Test
  public void testDescribeTopics() throws Exception {
    final List<String> topicNames = asList("topic");
    Assertions.assertThat(adminClient.describeTopics(topicNames).values().keySet()).containsAll(topicNames);
  }

  @Test
  public void testDescribeCluster() throws Exception {
    assertNotNull(adminClient.describeCluster());
  }

  @Test
  public void testDescribeAcls() throws Exception {
    assertEquals(emptySet(), adminClient.describeAcls(null).values().get());
  }

  @Test
  public void testCreateAcls() throws Exception {
    final List<AclBinding> bindings = asList(new AclBinding(new ResourcePattern(ResourceType.CLUSTER, "name", PatternType.LITERAL), 
                                                            new AccessControlEntry("principal", "host", AclOperation.ALL, AclPermissionType.ALLOW)));
    Assertions.assertThat(adminClient.createAcls(bindings).values().keySet()).containsAll(bindings);
  }

  @Test
  public void testDeleteAcls() throws Exception {
    assertEquals(emptyMap(), adminClient.deleteAcls(emptySet()).values());
  }

  @Test
  public void testDescribeConfigs() throws Exception {
    assertEquals(emptyMap(), adminClient.describeConfigs(emptySet()).values());
  }

  @Test
  public void testAlterConfigs() throws Exception {
    assertEquals(emptyMap(), adminClient.alterConfigs(emptyMap(), new AlterConfigsOptions()).values());
  }
  
  @Test
  public void testIncrementalAlterConfigs() throws Exception {
    assertEquals(emptyMap(), adminClient.incrementalAlterConfigs(emptyMap()).values());
  }

  @Test
  public void testAlterReplicaLogDirs() throws Exception {
    assertEquals(emptyMap(), adminClient.alterReplicaLogDirs(emptyMap()).values());
  }

  @Test
  public void testDescribeLogDirs() throws Exception {
    assertEquals(emptyMap(), adminClient.describeLogDirs(emptySet()).values());
  }

  @Test
  public void testDescribeReplicaLogDirs() throws Exception {
    assertEquals(emptyMap(), adminClient.describeReplicaLogDirs(emptySet()).values());
  }

  @Test
  public void testCreatePartitions() throws Exception {
    assertEquals(emptyMap(), adminClient.createPartitions(emptyMap()).values());
  }

  @Test
  public void testDeleteRecords() throws Exception {
    assertEquals(emptyMap(), adminClient.deleteRecords(emptyMap()).lowWatermarks());
  }

  @Test
  public void testCreateDelegationToken() throws Exception {
    assertEquals(null, adminClient.createDelegationToken().delegationToken().get());
  }

  @Test
  public void testRenewDelegationToken() throws Exception {
    assertEquals(0L, (long) adminClient.renewDelegationToken(new byte[0]).expiryTimestamp().get());
  }

  @Test
  public void testExpireDelegationToken() throws Exception {
    assertEquals(0L, (long) adminClient.expireDelegationToken(new byte[0]).expiryTimestamp().get());
  }

  @Test
  public void testDescribeDelegationToken() throws Exception {
    assertEquals(emptyList(), adminClient.describeDelegationToken().delegationTokens().get());
  }

  @Test
  public void testDescribeConsumerGroups() throws Exception {
    final List<String> groupIds = asList("group");
    Assertions.assertThat(adminClient.describeConsumerGroups(groupIds).describedGroups().keySet()).containsAll(groupIds);
  }

  @Test
  public void testListConsumerGroups() throws Exception {
    assertEquals(emptyList(), adminClient.listConsumerGroups().all().get());
  }

  @Test
  public void testDeleteConsumerGroups() throws Exception {
    assertEquals(emptyMap(), adminClient.deleteConsumerGroups(emptySet()).deletedGroups());
  }

  @Test
  public void testElectPreferredLeaders() throws Exception {
    assertNotNull(adminClient.electPreferredLeaders(emptySet()).partitions());
  }

  @Test
  public void testMetrics() throws Exception {
    assertEquals(emptyMap(), adminClient.metrics());
  }
}
