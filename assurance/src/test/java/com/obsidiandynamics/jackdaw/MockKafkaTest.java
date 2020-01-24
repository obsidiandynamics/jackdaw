package com.obsidiandynamics.jackdaw;

import static junit.framework.TestCase.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.internals.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.*;
import org.junit.runner.*;
import org.junit.runners.*;
import org.mockito.*;

import com.obsidiandynamics.assertion.*;
import com.obsidiandynamics.await.*;
import com.obsidiandynamics.func.*;
import com.obsidiandynamics.jackdaw.MockKafka.*;
import com.obsidiandynamics.jackdaw.SerdeProps.*;
import com.obsidiandynamics.junit.*;
import com.obsidiandynamics.threads.*;
import com.obsidiandynamics.worker.*;
import com.obsidiandynamics.worker.Terminator;

@RunWith(Parameterized.class)
public final class MockKafkaTest {
  @Parameterized.Parameters
  public static List<Object[]> data() {
    return TestCycle.timesQuietly(1);
  }

  private static final String TOPIC = "test";

  private final Timesert wait = Timesert.wait(10_000);

  private final List<Terminable> terminables = new ArrayList<>();

  @After
  public void after() {
    Terminator.of(terminables).terminate().joinSilently();
    terminables.clear();
  }

  @Test
  public void testProduceConsume() throws InterruptedException {
    testProduceConsume(10, 3, 0, 5);
  }

  @Test
  public void testWithRecordMapper() {
    ConsumerRecord<Object, Object> expectedRecord =
            new ConsumerRecord<>("topic", 0, 0, "key", "value");
    Kafka<Object, Object> mockKafka = new MockKafka<>()
            .withRecordMapper((objectObjectProducerRecord, recordMetadata) -> expectedRecord);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<Object, Object> producer = mockKafka.getProducer(props.producer());
    Consumer<Object, Object> consumer = mockKafka.getConsumer(new Properties());
    consumer.subscribe(Collections.singleton("topic"));
    producer.send(new ProducerRecord<>("topic", 0, "key", "value"));

    wait.until(() -> {
      ConsumerRecords<Object, Object> consumerRecords = consumer.poll(Duration.ofMillis(1));
      assertEquals(1, consumerRecords.count());
      assertSame(expectedRecord, consumerRecords.iterator().next());
    });
  }

  @Test
  public void testDefaultRecordMapping() {
    final MockKafka<Object, Object> mockKafka = new MockKafka<>();
    final RecordHeaders recordHeaders = new RecordHeaders(Collections.singleton(
                    new RecordHeader("headerKey", "headerValue".getBytes(StandardCharsets.UTF_8))));
    final RecordMetadata recordMetadata = new RecordMetadata(new TopicPartition("topic", 0),
            0, 0, 0, -1L, -1, -1);
    final ProducerRecord<Object, Object> producerRecord =
            new ProducerRecord<>("topic", 0, "key", "value", recordHeaders);

    final ConsumerRecord<Object, Object> consumerRecord = mockKafka.defaultRecordMapping(producerRecord, recordMetadata);

    assertEquals(producerRecord.topic(), consumerRecord.topic());
    assertEquals(producerRecord.partition().intValue(), consumerRecord.partition());
    assertEquals(producerRecord.key(), consumerRecord.key());
    assertEquals(producerRecord.value(), consumerRecord.value());
    assertEquals(producerRecord.headers(), consumerRecord.headers());
  }

  private static final class TestConsumer<K, V> implements Terminable {
    private final Kafka<K, V> kafka;

    private final WorkerThread thread;

    private Consumer<K, V> consumer;

    final KeyedBlockingQueue<Integer, ConsumerRecord<K, V>> received =
        new KeyedBlockingQueue<>(LinkedBlockingQueue::new);

    TestConsumer(Kafka<K, V> kafka) {
      this.kafka = kafka;
      thread = WorkerThread.builder()
          .withOptions(new WorkerOptions().daemon().withName(TestConsumer.class))
          .onStartup(this::startup)
          .onCycle(this::run)
          .onShutdown(this::shutdown)
          .buildAndStart();
    }

    private void startup(WorkerThread t) {
      consumer = kafka.getConsumer(new Properties());
      consumer.subscribe(Arrays.asList(TOPIC));
    }

    private void run(WorkerThread t) {
      final ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(1));
      records.forEach(r -> received.forKey(r.partition()).add(r));
    }

    private void shutdown(WorkerThread t, Throwable exception) {
      consumer.close();
    }

    @Override
    public Joinable terminate() {
      return thread.terminate();
    }
  }

  private void testProduceConsume(int messages, int partitions, int sendIntervalMillis, int numConsumers) throws InterruptedException {
    final int maxHistory = messages * partitions;
    final MockKafka<Integer, Integer> kafka = new MockKafka<>(partitions, maxHistory);
    final SerdeProps props = new SerdeProps(SerdePair.INTEGER);
    final MockProducer<Integer, Integer> producer = kafka.getProducer(props.producer());
    final List<TestConsumer<Integer, Integer>> consumers = new ArrayList<>(numConsumers);
    
    final AtomicInteger sent = new AtomicInteger();
    for (int m = 0; m < messages; m++) {
      for (int p = 0; p < partitions; p++) {
        producer.send(new ProducerRecord<>(TOPIC, p, m, m), (metadata, cause) -> sent.incrementAndGet());
      }
      
      if (consumers.size() < numConsumers) {
        consumers.add(new TestConsumer<>(kafka));
      }

      if (m != messages - 1 && sendIntervalMillis != 0) {
        Threads.sleep(sendIntervalMillis);
      }
    }

    final int expectedMessages = messages * partitions;
    assertEquals(expectedMessages, sent.get());

    while (consumers.size() < numConsumers) {
      consumers.add(new TestConsumer<>(kafka));
    }
    terminables.addAll(consumers);

    try {
      wait.untilTrue(() -> consumers.stream().filter(c -> c.received.totalSize() < expectedMessages).count() == 0);
    } finally {
      for (TestConsumer<Integer, Integer> consumer : consumers) {
        assertEquals(expectedMessages, consumer.received.totalSize());
        assertEquals(partitions, consumer.received.map().size());
        for (Map.Entry<Integer, BlockingQueue<ConsumerRecord<Integer, Integer>>> entry : consumer.received.map().entrySet()) {
          final List<ConsumerRecord<Integer, Integer>> records = new ArrayList<>(entry.getValue());
          assertEquals(messages, records.size());
          for (int m = 0; m < messages; m++) {
            final ConsumerRecord<Integer, Integer> cr = records.get(m);
            assertEquals(m, (int) cr.key());
            assertEquals(m, (int) cr.value());
          }
        }
      }
    }

    assertTrue("history.size=" + producer.history().size(), producer.history().size() <= maxHistory);

    producer.close();
  }

  @Test
  public void testSingletonProducer() {
    final MockKafka<String, String> kafka = new MockKafka<>(1, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);

    final Producer<String, String> p0 = kafka.getProducer(props.producer());
    assertNotNull(p0);

    final Producer<String, String> p1 = kafka.getProducer(props.producer());
    assertSame(p0, p1);
  }

  private static class TestRuntimeException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  @Test
  public void testDescribeProducer() {
    final LogLine logLine = mock(LogLine.class, Answers.CALLS_REAL_METHODS);
    new MockKafka<String, String>(1, 1).describeProducer(logLine, new Properties(), new Properties());
    verify(logLine).accept(notNull());
  }

  @Test(expected=TestRuntimeException.class)
  public void testSendWithRuntimeError() {
    final TestRuntimeException cause = new TestRuntimeException();
    final MockKafka<String, String> kafka = new MockKafka<String, String>(1, 1)
        .withSendRuntimeExceptionGenerator(__ -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    producer.send(new ProducerRecord<>("topic", "value"));
  }

  @Test
  public void testSendWithCallbackError() throws InterruptedException {
    final Exception cause = new Exception("simulated");
    final MockKafka<String, String> kafka = new MockKafka<String, String>(1, 1)
        .withSendCallbackExceptionGenerator(__ -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    final Callback callback = mock(Callback.class);
    final Future<RecordMetadata> f = producer.send(new ProducerRecord<>("topic", "value"), callback);
    assertTrue(f.isDone());
    try {
      f.get();
      fail("expected exception");
    } catch (ExecutionException e) {
      assertEquals(cause, e.getCause());
    }

    verify(callback).onCompletion(any(), eq(cause));
  }

  @Test
  public void testSendWithCallbackErrorWithoutCallback() throws InterruptedException {
    final Exception cause = new Exception("simulated");
    final MockKafka<String, String> kafka = new MockKafka<String, String>(1, 1)
        .withSendCallbackExceptionGenerator(__ -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    final Future<RecordMetadata> f = producer.send(new ProducerRecord<>("topic", "value"));
    assertTrue(f.isDone());
    try {
      f.get();
      fail("expected exception");
    } catch (ExecutionException e) {
      assertEquals(cause, e.getCause());
    }
  }

  @Test(expected=IllegalStateException.class)
  public void testSend_closedProducer() {
    final TestRuntimeException cause = new TestRuntimeException();
    final MockKafka<String, String> kafka = new MockKafka<String, String>(1, 1)
        .withSendRuntimeExceptionGenerator(__ -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    producer.close();
    producer.send(new ProducerRecord<>("topic", "value"));
  }

  @Test(expected=InvalidPartitionException.class)
  public void testSend_invalidPartition() {
    final MockKafka<String, String> kafka = new MockKafka<>(1, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    producer.send(new ProducerRecord<>("topic", 1, "key", "value"));
  }

  @Test
  public void testSend_clearBacklog() {
    final int maxHistory = 1;
    final MockKafka<String, String> kafka = new MockKafka<>(1, maxHistory);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());
    for (int i = 0; i < maxHistory + 1; i++) {
      producer.send(new ProducerRecord<>("topic", 0, "key", "value"));
    }
    assertEquals(maxHistory, kafka.getBacklog().size());
  }

  /**
   *  Tests attached/detached consumers in a group; we should be able receive on
   *  an attached consumer (the first one), but not a detached one (all others).
   */
  @Test
  public void testConsumeFromGroup() {
    final MockKafka<String, String> kafka = new MockKafka<>(1, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());

    final Properties consumerProps = new Properties();
    consumerProps.putAll(props.consumer());
    consumerProps.put("group.id", "group");

    final Consumer<String, String> attached = kafka.getConsumer(consumerProps);
    attached.subscribe(Arrays.asList("topic"), new NoOpConsumerRebalanceListener());

    final Consumer<String, String> detached = kafka.getConsumer(consumerProps);
    detached.subscribe(Arrays.asList("topic"));

    producer.send(new ProducerRecord<>("topic", 0, "key", "value"));
    wait.until(() -> assertEquals(1, attached.poll(Duration.ofMillis(1)).count()));

    Threads.sleep(10);
    assertEquals(0, detached.poll(Duration.ofMillis(1)).count());
  }

  /**
   *  Tests that a consumer subscribing twice to the same topic doesn't result in
   *  message duplication.
   */
  @Test
  public void testConsumeSubscribedTwice() {
    final MockKafka<String, String> kafka = new MockKafka<>(2, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());

    final Consumer<String, String> attached = kafka.getConsumer(props.consumer());
    attached.subscribe(Arrays.asList("topic"));
    attached.subscribe(Arrays.asList("topic"));

    producer.send(new ProducerRecord<>("topic", 0, "key", "value"));
    wait.until(() -> assertEquals(1, attached.poll(Duration.ofMillis(1)).count()));
  }

  /**
   *  Tests consumption of messages from a topic different from that of the publisher. In
   *  this particular scenario, the consumer subscribes to a topic <em>before</em> messages
   *  get published (to a different topic).
   */
  @Test
  public void testConsumeWrongTopicBeforePublish() {
    final MockKafka<String, String> kafka = new MockKafka<>(2, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());


    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("different"));
    producer.send(new ProducerRecord<>("topic", 0, "key", "value"));
    assertEquals(0, consumer.poll(Duration.ofMillis(1)).count());
  }

  /**
   *  Tests consumption of messages, having interrupted the consumer thread beforehand.
   */
  @Test
  public void testConsumeInterrupted() {
    final MockKafka<String, String> kafka = new MockKafka<>(2, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);


    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("different"));
    Thread.currentThread().interrupt();
    try {
      assertEquals(0, consumer.poll(Duration.ofMillis(10_000)).count());
    } finally {
      assertTrue(Thread.interrupted());
    }
  }

  /**
   *  Tests consumption of messages from a topic different from that of the publisher. In
   *  this particular scenario, the consumer subscribes to a topic <em>after</em> messages
   *  have been published (to a different topic).
   */
  @Test
  public void testConsumeWrongTopicAfterPublish() {
    final MockKafka<String, String> kafka = new MockKafka<>(2, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);
    final Producer<String, String> producer = kafka.getProducer(props.producer());

    producer.send(new ProducerRecord<>("topic", 0, "key", "value"));

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    consumer.subscribe(Arrays.asList("different"));
    assertEquals(0, consumer.poll(Duration.ofMillis(1)).count());
  }

  @Test
  public void testPartitionsFor_existingTopic() {
    final MockKafka<String, String> kafka = new MockKafka<>(1, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);

    final Producer<String, String> producer = kafka.getProducer(props.producer());
    producer.send(new ProducerRecord<>("topic", 0, "key", "value"));

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    final List<PartitionInfo> partitions = consumer.partitionsFor("topic");
    assertEquals(1, partitions.size());
  }

  @Test
  public void testPartitionsFor_nonExistentTopic() {
    final MockKafka<String, String> kafka = new MockKafka<>(1, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    final List<PartitionInfo> partitions = consumer.partitionsFor("topic");
    assertEquals(1, partitions.size());
  }

  @Test
  public void testCommitAsync() {
    final MockKafka<String, String> kafka = new MockKafka<>(1, 1);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    final OffsetCommitCallback callback = mock(OffsetCommitCallback.class);
    consumer.commitAsync(offsets, callback);
    wait.until(() -> verify(callback).onComplete(notNull(), isNull()));
  }

  @Test
  public void testCommitAsync_withError() {
    final Exception cause = new Exception("simulated");
    final MockKafka<String, String> kafka = new MockKafka<String, String>(1, 1)
        .withCommitExceptionGenerator(__ -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    final OffsetCommitCallback callback = mock(OffsetCommitCallback.class);
    consumer.commitAsync(offsets, callback);
    wait.until(() -> verify(callback).onComplete(notNull(), eq(cause)));
  }

  @Test
  public void testCommitAsync_withErrorNoCallback() {
    final Exception cause = new Exception("simulated");
    final MockKafka<String, String> kafka = new MockKafka<String, String>(1, 1)
        .withCommitExceptionGenerator(__ -> cause);
    final SerdeProps props = new SerdeProps(SerdePair.STRING);

    final Consumer<String, String> consumer = kafka.getConsumer(props.consumer());
    final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    consumer.commitAsync(offsets, null);
  }

  @Test
  public void testDescribeConsumer() {
    final LogLine logLine = mock(LogLine.class, Answers.CALLS_REAL_METHODS);
    new MockKafka<String, String>(1, 1).describeConsumer(logLine, new Properties(), new Properties());
    verify(logLine).accept(notNull());
  }

  @Test
  public void testGetAdminClient_defaultFactory() throws InterruptedException, ExecutionException {
    final AdminClient adminClient = new MockKafka<>().getAdminClient();
    assertSame(adminClient, PassiveAdminClient.getInstance());
  }

  @Test
  public void testGetAdminClient_customFactory() {
    final AdminClient adminClient = mock(AdminClient.class);
    final MockKafka<?, ?> kafka = new MockKafka<>().withAdminClientFactory(() -> adminClient);
    assertSame(adminClient, kafka.getAdminClient());
  }

  @Test
  public void testToString() {
    Assertions.assertToStringOverride(new MockKafka<>(1, 1));
  }
}
