package com.obsidiandynamics.jackdaw;

import static org.apache.kafka.clients.CommonClientConfigs.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.function.*;
import java.util.stream.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

/**
 *  A wrapper around the {@link AdminClient} class that offers synchronous/blocking
 *  semantics and provides transparent retry and error handling behaviour.
 */
public final class KafkaAdmin implements AutoCloseable {
  private Zlg zlg = Zlg.forDeclaringClass().get();

  private int retryAttempts = 60;

  /** The retry backoff interval, in milliseconds. */
  private int retryBackoff = 1_000;

  private final AdminClient admin;

  private KafkaAdmin(AdminClient admin) {
    this.admin = admin;
  }
  
  public static KafkaAdmin of(AdminClient admin) {
    return new KafkaAdmin(admin);
  }

  public KafkaAdmin withZlg(Zlg zlg) {
    this.zlg = zlg;
    return this;
  }

  public KafkaAdmin withRetryAttempts(int retryAttempts) {
    this.retryAttempts = retryAttempts;
    return this;
  }

  public KafkaAdmin withRetryBackoff(int retryBackoffMillis) {
    this.retryBackoff = retryBackoffMillis;
    return this;
  }

  /**
   *  Creates a {@link KafkaAdmin} wrapper for the given {@code config} and a factory for creating
   *  an {@link AdminClient} from a set of {@link Properties}.
   *  
   *  @param config The cluster configuration.
   *  @param adminClientFactory Factory for creating admin client instances.
   *  @return The {@link KafkaAdmin} wrapper.
   */
  public static KafkaAdmin forConfig(KafkaClusterConfig config, Function<Properties, AdminClient> adminClientFactory) {
    final String bootstrapServers = config.getCommonProps().getProperty(BOOTSTRAP_SERVERS_CONFIG);
    final Properties props = new PropsBuilder()
        .with(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        .build();
    final AdminClient admin = adminClientFactory.apply(props);
    return new KafkaAdmin(admin);
  }

  /**
   *  The result of waiting for the futures in {@link DescribeClusterResult}.
   */
  public static final class DescribeClusterOutcome {
    private final Collection<Node> nodes;
    private final Node controller;
    private final String clusterId;

    DescribeClusterOutcome(Collection<Node> nodes, Node controller, String clusterId) {
      this.nodes = nodes;
      this.controller = controller;
      this.clusterId = clusterId;
    }

    public Collection<Node> getNodes() {
      return nodes;
    }

    public Node getController() {
      return controller;
    }

    public String getClusterId() {
      return clusterId;
    }

    @Override
    public String toString() {
      return DescribeClusterOutcome.class.getSimpleName() + " [nodes=" + nodes + ", controller=" + controller + 
          ", clusterId=" + clusterId + "]";
    }
  }

  /**
   *  Describes the cluster, blocking until all operations have completed or a timeout occurs.
   *  
   *  @param timeoutMillis The timeout to wait for.
   *  @return The resulting {@link DescribeClusterOutcome}.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public DescribeClusterOutcome describeCluster(int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final DescribeClusterResult result = admin.describeCluster(new DescribeClusterOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.nodes(), result.controller(), result.clusterId());
      return new DescribeClusterOutcome(result.nodes().get(), result.controller().get(), result.clusterId().get());
    });
  }
  
  /**
   *  Lists non-internal topics in a cluster.
   *  
   *  @param timeoutMillis The timeout to wait for.
   *  @return A set of topic names.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public Set<String> listTopics(int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final ListTopicsResult result = admin.listTopics(new ListTopicsOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.listings());

      final Collection<TopicListing> listings = result.listings().get();
      return listings.stream().map(TopicListing::name).collect(Collectors.toSet());
    });
  }

  /**
   *  Ensures that the specified topics exist, creating missing ones if necessary. This method blocks 
   *  until all operations have completed or a timeout occurs.
   *  
   *  @param topics The topics to create.
   *  @param timeoutMillis The timeout to wait for.
   *  @return The set of topics that were created. (Absence from the set implies that the topic had already existed.)
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public Set<String> createTopics(Collection<NewTopic> topics, int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final CreateTopicsResult result = admin.createTopics(topics, 
                                                           new CreateTopicsOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.values().values());

      final Set<String> created = new HashSet<>(topics.size(), 1f);
      for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
        try {
          entry.getValue().get();
          zlg.d("Created topic %s", z -> z.arg(entry::getKey));
          created.add(entry.getKey());
        } catch (ExecutionException e) {
          if (e.getCause() instanceof TopicExistsException) {
            zlg.d("Topic %s already exists", z -> z.arg(entry::getKey));
          } else {
            throw e;
          }
        }
      }
      return created;
    });
  }
  
  /**
   *  Queues the specified topics for deletion if they exist. This method blocks until all operations have 
   *  completed or a timeout occurs. <br>
   *  
   *  <b>Note:</b> even though this is a synchronous operation, the effect is merely the marking
   *  of the topic for deletion on the broker. The actual deletion of a marked topic happens 
   *  asynchronously, and at the discretion of the broker. The topic may linger for some time
   *  after this method returns.
   *  
   *  @param topics The topics to delete.
   *  @param timeoutMillis The timeout to wait for.
   *  @return The set of topics that were marked for deletion. (Absence from the set implies that the topic didn't exist.)
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public Set<String> deleteTopics(Collection<String> topics, int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final DeleteTopicsResult result = admin.deleteTopics(topics, 
                                                           new DeleteTopicsOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.values().values());

      final Set<String> deleted = new HashSet<>(topics.size(), 1f);
      for (Map.Entry<String, KafkaFuture<Void>> entry : result.values().entrySet()) {
        try {
          entry.getValue().get();
          zlg.d("Deleted topic %s", z -> z.arg(entry::getKey));
          deleted.add(entry.getKey());
        } catch (ExecutionException e) {
          if (e.getCause() instanceof UnknownTopicOrPartitionException) {
            zlg.d("Topic %s does not exist", z -> z.arg(entry::getKey));
          } else {
            throw e;
          }
        }
      }
      return deleted;
    });
  }
  
  /**
   *  Lists consumer groups.
   *  
   *  @param timeoutMillis The timeout to wait for.
   *  @return A set of group IDs.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public Set<String> listConsumerGroups(int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final ListConsumerGroupsResult result = admin.listConsumerGroups(new ListConsumerGroupsOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.all());

      final Collection<ConsumerGroupListing> listings = result.all().get();
      return listings.stream().map(ConsumerGroupListing::groupId).collect(Collectors.toSet());
    });
  }
  
  /**
   *  Lists consumer group offsets.
   *  
   *  @param groupId The group ID.
   *  @param timeoutMillis The timeout to wait for.
   *  @return The offsets.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public Map<TopicPartition, OffsetAndMetadata> listConsumerGroupOffsets(String groupId, int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final ListConsumerGroupOffsetsResult result = admin.listConsumerGroupOffsets(groupId, new ListConsumerGroupOffsetsOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.partitionsToOffsetAndMetadata());

      return result.partitionsToOffsetAndMetadata().get();
    });
  }

  /**
   *  Queues the specified consumer groups for deletion if they exist. This method blocks until all operations have 
   *  completed or a timeout occurs. <br>
   *  
   *  <b>Note:</b> even though this is a synchronous operation, the effect is merely the marking
   *  of the group for deletion on the broker. The actual deletion of a marked group happens 
   *  asynchronously, and at the discretion of the broker. The topic may linger for some time
   *  after this method returns.
   *  
   *  @param groups The group IDs to delete.
   *  @param timeoutMillis The timeout to wait for.
   *  @return The set of groups that were marked for deletion. (Absence from the set implies that the group didn't exist.)
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws InterruptedException If the thread was interrupted.
   *  @throws TimeoutException If a timeout occurred.
   */
  public Set<String> deleteConsumerGroups(Collection<String> groups, int timeoutMillis) throws ExecutionException, TimeoutException, InterruptedException {
    return runWithRetry(() -> {
      final DeleteConsumerGroupsResult result = admin.deleteConsumerGroups(groups, 
                                                                           new DeleteConsumerGroupsOptions().timeoutMs(timeoutMillis));
      awaitFutures(timeoutMillis, result.deletedGroups().values());

      final Set<String> deleted = new HashSet<>(groups.size(), 1f);
      for (Map.Entry<String, KafkaFuture<Void>> entry : result.deletedGroups().entrySet()) {
        try {
          entry.getValue().get();
          zlg.d("Deleted consumer group %s", z -> z.arg(entry::getKey));
          deleted.add(entry.getKey());
        } catch (ExecutionException e) {
          if (e.getCause() instanceof GroupNotEmptyException) {
            zlg.d("Group %s not empty (will be deleted later)", z -> z.arg(entry::getKey));
            deleted.add(entry.getKey());
          } else if (e.getCause() instanceof GroupIdNotFoundException) {
            zlg.d("Group %s not found", z -> z.arg(entry::getKey));
          }  else {
            throw e;
          }
        }
      }
      return deleted;
    });
  }
  
  static final class UnhandledException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    UnhandledException(Throwable cause) { super(cause); }
  }

  <T> T runWithRetry(CheckedSupplier<T, Throwable> supplier) throws ExecutionException, TimeoutException, InterruptedException {
    try {
      return new Retry()
          .withExceptionMatcher(Retry.hasCauseThat(Retry.isA(RetriableException.class)))
          .withBackoff(retryBackoff)
          .withAttempts(retryAttempts)
          .withFaultHandler(zlg::w)
          .withErrorHandler(zlg::e)
          .run(supplier);
    } catch (Throwable e) {
      if (e instanceof ExecutionException) {
        throw (ExecutionException) e;
      } else if (e instanceof RuntimeException) {
        throw (RuntimeException) e;
      } else if (e instanceof InterruptedException) {
        throw (InterruptedException) e;
      } else if (e instanceof TimeoutException) {
        throw (TimeoutException) e;
      } else {
        throw new UnhandledException(e);
      }
    }
  }

  @Override
  public void close() {
    close(Duration.ofMillis(Long.MAX_VALUE));
  }

  public void close(Duration duration) {
    admin.close(duration);
  }

  public static void awaitFutures(long timeout, KafkaFuture<?>... futures) throws TimeoutException, InterruptedException {
    awaitFutures(timeout, Arrays.asList(futures));
  }

  public static void awaitFutures(long timeoutMillis, Collection<? extends KafkaFuture<?>> futures) throws TimeoutException, InterruptedException {
    for (KafkaFuture<?> future : futures) {
      try {
        // allow for a 50% grace period to avoid a premature timeout (let the AdminClient time out first)
        future.get(timeoutMillis * 3 / 2, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new TimeoutException(e.getCause().getMessage());
        }
      }
    }
  }
}
