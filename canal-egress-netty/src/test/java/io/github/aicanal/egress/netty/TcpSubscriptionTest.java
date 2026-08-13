package io.github.aicanal.egress.netty;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.model.*;
import io.github.aicanal.cluster.*;
import io.github.aicanal.storage.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;

class TcpSubscriptionTest {
  @Test
  void rejectsAckBeyondSentAndAfterLeadershipLoss() {
    MemoryStore store = new MemoryStore();
    InMemoryLeaderGuard g = new InMemoryLeaderGuard();
    Leadership l = new Leadership("d", "n", 3);
    g.onAcquired(l);
    TcpSubscription s = new TcpSubscription("d", "c", store, g);
    assertEquals(1, s.fetch(10, 100).size());
    assertThrows(IllegalArgumentException.class, () -> s.ack(2));
    assertEquals(1, s.ack(1).getCommittedOffset());
    g.onRevoked(l);
    assertThrows(NotLeaderException.class, () -> s.fetch(1, 100));
  }

  static final class MemoryStore implements EventStore {
    DeliveryCheckpoint cp = DeliveryCheckpoint.initial("d", "tcp:c", 3);
    final StoredEvent event =
        new StoredEvent(
            new CanalEvent(
                "e",
                "d",
                1,
                "k",
                "c",
                Instant.EPOCH,
                Instant.EPOCH,
                1,
                Collections.emptyMap(),
                new byte[1],
                "x"),
            StoredEvent.State.READY,
            Instant.EPOCH,
            null,
            0,
            "",
            "v",
            "n");

    public List<StoredEvent> readAfter(long o, int l, int b) {
      return o < 1 ? Collections.singletonList(event) : Collections.emptyList();
    }

    public DeliveryCheckpoint checkpoint(String c, long e) {
      return cp;
    }

    public DeliveryCheckpoint commitDelivery(String c, long o, long v, long e) {
      cp = cp.advance(o, e);
      return cp;
    }

    public IngressAck appendIngress(AgentPublishRequest r, Durability d) {
      throw new UnsupportedOperationException();
    }

    public StoredEvent appendReady(long i, int x, CanalEvent e) {
      throw new UnsupportedOperationException();
    }

    public void appendRejected(long i, int x, String c, String s) {}

    public Optional<StoredEvent> findByEventId(String i) {
      return Optional.empty();
    }

    public Optional<IngestRecord> findIngress(long i) {
      return Optional.empty();
    }

    public boolean isProcessed(long i, int x) {
      return false;
    }

    public RecoveryPlan recover() {
      return new RecoveryPlan(Collections.emptyList(), 0, 1);
    }

    public void flush() {}

    public void close() {}
  }
}
