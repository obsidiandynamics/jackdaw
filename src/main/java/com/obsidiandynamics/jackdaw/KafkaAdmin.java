package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.jackdaw.KafkaClusterConfig.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.function.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;

import com.obsidiandynamics.func.*;
import com.obsidiandynamics.retry.*;
import com.obsidiandynamics.yconf.util.*;
import com.obsidiandynamics.zerolog.*;

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

  public static KafkaAdmin forConfig(KafkaClusterConfig config, Function<Properties, AdminClient> adminClientFactory) {
    final String bootstrapServers = config.getCommonProps().getProperty(CONFIG_BOOTSTRAP_SERVERS);
    final Properties props = new PropsBuilder()
        .with(CONFIG_BOOTSTRAP_SERVERS, bootstrapServers)
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
  }

  /**
   *  Describes the cluster, blocking until all operations have completed or a timeout occurs.
   *  
   *  @param timeoutMillis The timeout to wait for.
   *  @return The {@link DescribeClusterOutcome}.
   *  @throws InterruptedException If the thread was interrupted while waiting for the create outcome.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws TimeoutException If the request timed out.
   */
  public DescribeClusterOutcome describeCluster(long timeoutMillis) throws TimeoutException, InterruptedException, ExecutionException {
    return runWithRetry(() -> {
      final DescribeClusterResult result = admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeoutMillis));
      awaitFutures(timeoutMillis, result.nodes(), result.controller(), result.clusterId());
      return new DescribeClusterOutcome(result.nodes().get(), result.controller().get(), result.clusterId().get());
    });
  }

  /**
   *  Ensures that the given topics exist, creating missing ones if necessary. This method blocks until all operations
   *  have completed or a timeout occurs.
   *  
   *  @param topics The topics.
   *  @param timeoutMillis The timeout to wait for.
   *  @return The set of topics that were created. (Absence from the set implies that the topic had already existed.)
   *  @throws InterruptedException If the thread was interrupted while waiting for the create outcome.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws TimeoutException If the request timed out.
   */
  public Set<String> ensureTopicsExist(Collection<NewTopic> topics, long timeoutMillis) throws InterruptedException, ExecutionException, TimeoutException {
    return runWithRetry(() -> {
      final CreateTopicsResult result = admin.createTopics(topics, 
                                                           new CreateTopicsOptions().timeoutMs((int) timeoutMillis));
      awaitFutures(timeoutMillis, result.values().values());

      final Set<String> created = new HashSet<>();
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

  static final class UnhandledException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    UnhandledException(Throwable cause) { super(cause); }
  }

  <T> T runWithRetry(CheckedSupplier<T, Throwable> supplier) throws ExecutionException {
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
      } else {
        throw new UnhandledException(e);
      }
    }
  }

  @Override
  public void close() {
    close(0, TimeUnit.MILLISECONDS);
  }

  public void close(long duration, TimeUnit unit) {
    admin.close(duration, unit);
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
