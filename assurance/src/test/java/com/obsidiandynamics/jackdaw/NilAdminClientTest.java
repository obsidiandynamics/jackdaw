package com.obsidiandynamics.jackdaw;

import static java.util.Collections.*;
import static org.junit.Assert.*;

import java.util.concurrent.*;

import org.junit.*;

public final class NilAdminClientTest {
  private NilAdminClient adminClient;
  
  @Before
  public void before() {
    adminClient = NilAdminClient.getInstance();
  }
  
  @Test
  public void testSingletion() {
    assertSame(adminClient, NilAdminClient.getInstance());
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
    assertEquals(emptyMap(), adminClient.describeTopics(emptySet()).values());
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
    assertEquals(emptyMap(), adminClient.createAcls(emptySet()).values());
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
    assertEquals(emptyMap(), adminClient.alterConfigs(emptyMap()).values());
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
    assertEquals(emptyMap(), adminClient.describeConsumerGroups(emptySet()).describedGroups());
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
