package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.internals.*;
import org.junit.*;

import com.obsidiandynamics.jackdaw.KafkaAdmin.*;

public final class KafkaAdminTest {
  private KafkaAdmin admin;
  
  @After
  public void after() {
    if (admin != null) admin.close();
  }
  
  @Test
  public void testAwaitFuturesPass() throws TimeoutException, InterruptedException {
    final KafkaFuture<Void> f = KafkaFuture.completedFuture(null);
    KafkaAdmin.awaitFutures(10, f);
  }
  
  @Test(expected=TimeoutException.class)
  public void testAwaitFuturesPassTimeoutException() throws TimeoutException, InterruptedException {
    final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
    f.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException());
    KafkaAdmin.awaitFutures(10, f);
  }
  
  @Test
  public void testAwaitFuturesPassOtherException() throws TimeoutException, InterruptedException {
    final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
    f.completeExceptionally(new RuntimeException());
    KafkaAdmin.awaitFutures(10, f);
  }
  
  @Test
  public void testEnsureExistsNewTopic() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = new KafkaAdmin(client);
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      futures.put("test", KafkaFuture.completedFuture(null));
      when(r.values()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.ensureExists(TestTopic.newOf("test"), 1_000);
    assertTrue(topics.contains("test"));
  }
  
  @Test
  public void testEnsureExistsWithExisting() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = new KafkaAdmin(client);
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.TopicExistsException("testEnsureExistsWithExisting"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.ensureExists(TestTopic.newOf("test"), 1_000);
    assertFalse(topics.contains("test"));
  }
  
  @Test(expected=ExecutionException.class)
  public void testEnsureExistsWithException() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = new KafkaAdmin(client);
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.AuthorizationException("testEnsureExistsWithException"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    admin.ensureExists(TestTopic.newOf("test"), 1_000);
  }
  
  @Test
  public void testDescribeCluster() throws TimeoutException, InterruptedException, ExecutionException {
    final AdminClient client = mock(AdminClient.class);
    admin = new KafkaAdmin(client);
    when(client.describeCluster(any())).then(invocation -> {
      final DescribeClusterResult result = mock(DescribeClusterResult.class);
      when(result.nodes()).thenReturn(KafkaFuture.completedFuture(Collections.singleton(Node.noNode())));
      when(result.controller()).thenReturn(KafkaFuture.completedFuture(Node.noNode()));
      when(result.clusterId()).thenReturn(KafkaFuture.completedFuture("test-cluster"));
      return result;
    });
    final DescribeClusterOutcome o = admin.describeCluster(1_000);
    assertEquals(Collections.singleton(Node.noNode()), o.getNodes());
    assertEquals(Node.noNode(), o.getController());
    assertEquals("test-cluster", o.getClusterId());
  }
  
  @Test
  public void testForConfig() {
    final AdminClient client = mock(AdminClient.class);
    final Function<Properties, AdminClient> factory = props -> client;
    final KafkaAdmin admin = KafkaAdmin.forConfig(new KafkaClusterConfig(), factory);
    admin.close();
    verify(client).close(anyLong(), any());
  }
}
