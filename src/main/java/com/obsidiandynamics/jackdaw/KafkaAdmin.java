package com.obsidiandynamics.jackdaw;

import static com.obsidiandynamics.jackdaw.KafkaClusterConfig.*;

import java.lang.invoke.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeoutException;
import java.util.function.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.*;

import com.obsidiandynamics.yconf.props.*;
import com.obsidiandynamics.zerolog.*;

public final class KafkaAdmin implements AutoCloseable {
  private static final Zlg zlg = Zlg.forClass(MethodHandles.lookup().lookupClass()).get();
  
  private final AdminClient admin;
  
  public KafkaAdmin(AdminClient admin) {
    this.admin = admin;
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
   *  @return The outcome.
   *  @throws InterruptedException If the thread was interrupted while waiting for the create outcome.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws TimeoutException If the request timed out.
   */
  public DescribeClusterOutcome describeCluster(long timeoutMillis) throws TimeoutException, InterruptedException, ExecutionException {
    final DescribeClusterResult result = admin.describeCluster(new DescribeClusterOptions().timeoutMs((int) timeoutMillis));
    awaitFutures(timeoutMillis, result.nodes(), result.controller(), result.clusterId());
    return new DescribeClusterOutcome(result.nodes().get(), result.controller().get(), result.clusterId().get());
  }
  
  /**
   *  Ensures that a given topic exists, creating one if necessary. This method blocks until all operations
   *  have completed or a timeout occurs.
   *  
   *  @param topic The topic.
   *  @param timeoutMillis The timeout to wait for.
   *  @return The set of topics that were created. (Absence from the set implies that the topic had already existed.)
   *  @throws InterruptedException If the thread was interrupted while waiting for the create outcome.
   *  @throws ExecutionException If an unexpected error occurred.
   *  @throws TimeoutException If the request timed out.
   */
  public Set<String> ensureExists(NewTopic topic, long timeoutMillis) throws InterruptedException, ExecutionException, TimeoutException {
    final CreateTopicsResult result = admin.createTopics(Collections.singleton(topic), 
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
        // allow for a 50% grace period to avoid a premature timeout (let the AdminClient timeout first)
        future.get(timeoutMillis * 3 / 2, TimeUnit.MILLISECONDS);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
          throw new TimeoutException(e.getCause().getMessage());
        }
      }
    }
  }
}
