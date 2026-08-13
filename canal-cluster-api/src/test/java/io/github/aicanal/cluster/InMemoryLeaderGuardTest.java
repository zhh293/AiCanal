package io.github.aicanal.cluster;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class InMemoryLeaderGuardTest {
  @Test
  void revocationAndEpochAreFailClosed() {
    InMemoryLeaderGuard g = new InMemoryLeaderGuard();
    Leadership one = new Leadership("d", "n", 1);
    g.onAcquired(one);
    assertTrue(g.isLeader("d", 1));
    g.onAcquired(new Leadership("d", "n", 2));
    g.onRevoked(one);
    assertTrue(g.isLeader("d", 2));
    g.onRevoked(new Leadership("d", "n", 2));
    assertFalse(g.isLeader("d", 2));
    assertThrows(NotLeaderException.class, () -> g.requireLeadership("d"));
  }
}
