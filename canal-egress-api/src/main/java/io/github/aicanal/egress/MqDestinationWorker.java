package io.github.aicanal.egress;

import io.github.aicanal.api.model.DeliveryCheckpoint;
import io.github.aicanal.cluster.*;
import io.github.aicanal.storage.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class MqDestinationWorker implements EgressRuntime, Runnable {
  public enum DeadLetterPolicy {
    BLOCK,
    SKIP
  }

  private final String destination, channelId;
  private final EventStore store;
  private final DestinationLeaderGuard guard;
  private final MessageQueueProducer producer;
  private final DeliveryDeadLetterStore deadLetters;
  private final int batchSize, maxBatchBytes, maxAttempts;
  private final long initialBackoffMillis, maxBackoffMillis;
  private final DeadLetterPolicy policy;
  private final AtomicBoolean running = new AtomicBoolean();
  private volatile Thread thread;
  private volatile String state = "NEW";

  public MqDestinationWorker(
      String destination,
      String channelId,
      EventStore store,
      DestinationLeaderGuard guard,
      MessageQueueProducer producer,
      DeliveryDeadLetterStore deadLetters,
      int batchSize,
      int maxBatchBytes,
      int maxAttempts,
      Duration initialBackoff,
      Duration maxBackoff,
      DeadLetterPolicy policy) {
    this.destination = destination;
    this.channelId = channelId;
    this.store = store;
    this.guard = guard;
    this.producer = producer;
    this.deadLetters = deadLetters;
    this.batchSize = batchSize;
    this.maxBatchBytes = maxBatchBytes;
    this.maxAttempts = maxAttempts;
    this.initialBackoffMillis = initialBackoff.toMillis();
    this.maxBackoffMillis = maxBackoff.toMillis();
    this.policy = policy;
  }

  public void start() {
    if (running.compareAndSet(false, true)) {
      thread = new Thread(this, "mq-worker-" + destination);
      thread.setDaemon(true);
      thread.start();
    }
  }

  public String state() {
    return state;
  }

  public void run() {
    while (running.get()) {
      try {
        Leadership l = guard.requireLeadership(destination);
        state = "RUNNING";
        DeliveryCheckpoint cp = store.checkpoint(channelId, l.getEpoch());
        List<StoredEvent> events =
            store.readAfter(cp.getCommittedOffset(), batchSize, maxBatchBytes);
        if (events.isEmpty()) {
          sleep(100);
          continue;
        }
        MessageBatch batch = new MessageBatch(events);
        boolean sent = false;
        String failure = "unconfirmed by broker";
        int attempts = 0;
        long delay = initialBackoffMillis;
        for (int attempt = 1; attempt <= maxAttempts && running.get(); attempt++) {
          attempts = attempt;
          if (!guard.isLeader(destination, l.getEpoch())) throw new NotLeaderException(destination);
          SendResult result =
              producer.send(
                  batch,
                  new SendContext(
                      destination,
                      l.getEpoch(),
                      events.get(0).getEvent().getEventId() + ":" + batch.lastOffset()));
          if (result.isConfirmed()) {
            sent = true;
            break;
          }
          failure = result.getDetail();
          if (!result.isRetryable()) break;
          sleep(delay);
          delay = Math.min(maxBackoffMillis, Math.max(delay + 1, delay * 2));
        }
        if (!sent) {
          deadLetters.persist(batch, failure, attempts);
          state = policy == DeadLetterPolicy.BLOCK ? "DEAD_LETTER_BLOCKED" : "DEAD_LETTER_SKIPPED";
          if (policy == DeadLetterPolicy.BLOCK) {
            sleep(1000);
            continue;
          }
        }
        if (!guard.isLeader(destination, l.getEpoch())) throw new NotLeaderException(destination);
        store.commitDelivery(channelId, batch.lastOffset(), cp.getVersion(), l.getEpoch());
      } catch (NotLeaderException e) {
        state = "STANDBY";
        sleep(250);
      } catch (RuntimeException e) {
        state = "RETRYING";
        sleep(500);
      }
    }
    state = "TERMINATED";
  }

  private void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      running.set(false);
    }
  }

  public void close() {
    running.set(false);
    if (thread != null) thread.interrupt();
    producer.flush(Duration.ofSeconds(10));
    producer.close();
    deadLetters.close();
  }
}
