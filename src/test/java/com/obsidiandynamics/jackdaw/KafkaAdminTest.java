package com.obsidiandynamics.jackdaw;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.internals.*;
import org.apache.kafka.common.record.*;
import org.junit.*;

import com.obsidiandynamics.jackdaw.KafkaAdmin.*;
import com.obsidiandynamics.verifier.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaAdminTest {
  private KafkaAdmin admin;

  @After
  public void after() {
    if (admin != null) admin.close();
  }
  
  @Test
  public void testDescribeClusterOutcomePojo() {
    PojoVerifier.forClass(DescribeClusterOutcome.class).verify();
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
  public void testCreateTopicsNewTopic() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      futures.put("test", KafkaFuture.completedFuture(null));
      when(r.values()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.createTopics(Collections.singleton(TestTopic.newOf("test")), 1_000);
    assertTrue(topics.contains("test"));
  }

  @Test
  public void testCreateTopicsWithExisting() throws InterruptedException, ExecutionException, TimeoutException {
    final MockLogTarget logTarget = new MockLogTarget();
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client).withZlg(logTarget.logger());
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.TopicExistsException("simulated"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.createTopics(Collections.singleton(TestTopic.newOf("test")), 1_000);
    assertFalse(topics.contains("test"));
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("exists").assertCount(1);
  }

  @Test(expected=ExecutionException.class)
  public void testCreateTopicsWithException() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.AuthorizationException("simulated"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    admin.createTopics(Collections.singleton(TestTopic.newOf("test")), 1_000);
  }

  @Test
  public void testCreateTopicsWithRetriableExceptionResolved() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    final MockLogTarget logTarget = new MockLogTarget();
    admin = KafkaAdmin.of(client).withRetryAttempts(2).withRetryBackoff(0).withZlg(logTarget.logger());
    final AtomicInteger invocationAttempts = new AtomicInteger();
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      if (invocationAttempts.getAndIncrement() == 0) {
        f.completeExceptionally(new NotControllerException("simulated"));
      } else {
        f.complete(null);
      }
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    admin.createTopics(Collections.singleton(TestTopic.newOf("test")), 1_000);
    assertEquals(2, invocationAttempts.get());
    logTarget.entries().forLevel(LogLevel.WARN).containing("attempt").assertCount(1);
  }

  @Test(expected=ExecutionException.class)
  public void testCreateTopicsWithRetriableExceptionNotResolved() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client).withRetryAttempts(2).withRetryBackoff(0).withZlg(Zlg.nop());
    when(client.createTopics(any(), any())).then(invocation -> {
      final CreateTopicsResult r = mock(CreateTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new NotControllerException("simulated"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    admin.createTopics(Collections.singleton(TestTopic.newOf("test")), 1_000);
  }

  @Test
  public void testDescribeCluster() throws TimeoutException, InterruptedException, ExecutionException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
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
  public void testDeleteTopicsExistingTopic() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.deleteTopics(any(), any())).then(invocation -> {
      final DeleteTopicsResult r = mock(DeleteTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      futures.put("test", KafkaFuture.completedFuture(null));
      when(r.values()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.deleteTopics(Collections.singleton("test"), 1_000);
    assertTrue(topics.contains("test"));
  }

  @Test
  public void testDeleteTopicsWithNonExistent() throws InterruptedException, ExecutionException, TimeoutException {
    final MockLogTarget logTarget = new MockLogTarget();
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client).withZlg(logTarget.logger());
    when(client.deleteTopics(any(), any())).then(invocation -> {
      final DeleteTopicsResult r = mock(DeleteTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.UnknownTopicOrPartitionException("simulated"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.deleteTopics(Collections.singleton("test"), 1_000);
    assertFalse(topics.contains("test"));
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("does not exist").assertCount(1);
  }

  @Test(expected=ExecutionException.class)
  public void testDeleteTopicsWithException() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.deleteTopics(any(), any())).then(invocation -> {
      final DeleteTopicsResult r = mock(DeleteTopicsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.AuthorizationException("simulated"));
      futures.put("test", f);
      when(r.values()).thenReturn(futures);
      return r; 
    });
    admin.deleteTopics(Collections.singleton("test"), 1_000);
  }

  @Test
  public void testListTopics() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.listTopics(any())).then(invocation -> {
      final ListTopicsResult r = mock(ListTopicsResult.class);
      final TopicListing listing = new TopicListing("test", false);
      when(r.listings()).thenReturn(KafkaFuture.completedFuture(Collections.singletonList(listing)));
      return r; 
    });
    final Set<String> topics = admin.listTopics(1_000);
    assertTrue(topics.contains("test"));
  }

  @Test
  public void testListConsumerGroups() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.listConsumerGroups(any())).then(invocation -> {
      final ListConsumerGroupsResult r = mock(ListConsumerGroupsResult.class);
      final ConsumerGroupListing listing = new ConsumerGroupListing("test", false);
      when(r.all()).thenReturn(KafkaFuture.completedFuture(Collections.singletonList(listing)));
      return r; 
    });
    final Set<String> topics = admin.listConsumerGroups(1_000);
    assertTrue(topics.contains("test"));
  }

  @Test
  public void testDeleteConsumerGroupsExistingGroup() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.deleteConsumerGroups(any(), any())).then(invocation -> {
      final DeleteConsumerGroupsResult r = mock(DeleteConsumerGroupsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      futures.put("test", KafkaFuture.completedFuture(null));
      when(r.deletedGroups()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.deleteConsumerGroups(Collections.singleton("test"), 1_000);
    assertTrue(topics.contains("test"));
  }

  @Test
  public void testDeleteConsumerGroupsWithGroupNotEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    final MockLogTarget logTarget = new MockLogTarget();
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client).withZlg(logTarget.logger());
    when(client.deleteConsumerGroups(any(), any())).then(invocation -> {
      final DeleteConsumerGroupsResult r = mock(DeleteConsumerGroupsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.GroupNotEmptyException("simulated"));
      futures.put("test", f);
      when(r.deletedGroups()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.deleteConsumerGroups(Collections.singleton("test"), 1_000);
    assertTrue(topics.contains("test"));
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("not empty").assertCount(1);
  }

  @Test
  public void testDeleteConsumerGroupsWithGroupNotFound() throws InterruptedException, ExecutionException, TimeoutException {
    final MockLogTarget logTarget = new MockLogTarget();
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client).withZlg(logTarget.logger());
    when(client.deleteConsumerGroups(any(), any())).then(invocation -> {
      final DeleteConsumerGroupsResult r = mock(DeleteConsumerGroupsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.GroupIdNotFoundException("simulated"));
      futures.put("test", f);
      when(r.deletedGroups()).thenReturn(futures);
      return r; 
    });
    final Set<String> topics = admin.deleteConsumerGroups(Collections.singleton("test"), 1_000);
    assertFalse(topics.contains("test"));
    logTarget.entries().forLevel(LogLevel.DEBUG).containing("not found").assertCount(1);
  }

  @Test(expected=ExecutionException.class)
  public void testDeleteConsumerGroupsWithException() throws InterruptedException, ExecutionException, TimeoutException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client);
    when(client.deleteConsumerGroups(any(), any())).then(invocation -> {
      final DeleteConsumerGroupsResult r = mock(DeleteConsumerGroupsResult.class);
      final Map<String, KafkaFuture<Void>> futures = new HashMap<>();
      final KafkaFutureImpl<Void> f = new KafkaFutureImpl<>();
      f.completeExceptionally(new org.apache.kafka.common.errors.AuthorizationException("simulated"));
      futures.put("test", f);
      when(r.deletedGroups()).thenReturn(futures);
      return r; 
    });
    admin.deleteConsumerGroups(Collections.singleton("test"), 1_000);
  }

  @Test
  public void testForConfig() {
    final AdminClient client = mock(AdminClient.class);
    final Function<Properties, AdminClient> factory = props -> client;
    final KafkaAdmin admin = KafkaAdmin.forConfig(new KafkaClusterConfig(), factory);
    admin.close();
    verify(client).close(anyLong(), any());
  }

  @Test(expected=ExecutionException.class)
  public void testRunWithRetryThrowsExecutionException() throws ExecutionException, TimeoutException, InterruptedException {
    final AdminClient client = mock(AdminClient.class);
    KafkaAdmin.of(client).runWithRetry(() -> {
      throw new ExecutionException("simulated", null);
    });
  }

  @Test(expected=RetriableException.class)
  public void testRunWithRetryThrowsRetriableException() throws ExecutionException, TimeoutException, InterruptedException {
    final AdminClient client = mock(AdminClient.class);
    KafkaAdmin.of(client).runWithRetry(() -> {
      throw new InvalidRecordException("simulated");
    });
  }

  @Test(expected=TimeoutException.class)
  public void testRunWithRetryThrowsTimeoutException() throws ExecutionException, TimeoutException, InterruptedException {
    final AdminClient client = mock(AdminClient.class);
    KafkaAdmin.of(client).runWithRetry(() -> {
      throw new TimeoutException("simulated");
    });
  }

  @Test(expected=InterruptedException.class)
  public void testRunWithRetryThrowsInterruptedException() throws ExecutionException, TimeoutException, InterruptedException {
    final AdminClient client = mock(AdminClient.class);
    KafkaAdmin.of(client).runWithRetry(() -> {
      throw new InterruptedException("simulated");
    });
  }

  @Test(expected=UnhandledException.class)
  public void testRunWithRetryThrowsIOException() throws ExecutionException, TimeoutException, InterruptedException {
    final AdminClient client = mock(AdminClient.class);
    admin = KafkaAdmin.of(client).runWithRetry(() -> {
      throw new IOException("simulated");
    });
  }
}
