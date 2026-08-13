package io.github.aicanal.cluster.raft;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.cluster.*;
import io.github.aicanal.spi.PluginContext;
import java.io.IOException;
import java.net.DatagramSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftLeaderElectorTest {
  @TempDir Path temporary;

  @Test
  void electsOneLeaderFailsOverAndFencesMinority() throws Exception {
    int[] ports = {freePort(), freePort(), freePort()};
    List<String> peers = peers(ports);
    List<Node> nodes = new ArrayList<>();
    try {
      for (int i = 0; i < 3; i++) nodes.add(start("n" + (i + 1), ports[i], peers, "resources"));

      await(Duration.ofSeconds(8), () -> leaders(nodes) == 1);
      Node first = nodes.stream().filter(Node::isLeader).findFirst().orElseThrow();
      long firstTerm = first.leadership.get().getEpoch();

      first.close();
      await(Duration.ofSeconds(8), () -> leaders(nodes) == 1);
      Node replacement = nodes.stream().filter(Node::isLeader).findFirst().orElseThrow();
      assertNotSame(first, replacement);
      assertTrue(replacement.leadership.get().getEpoch() > firstTerm);

      for (Node node : nodes) if (node != replacement && node.open.get()) node.close();
      await(Duration.ofSeconds(5), () -> !replacement.isLeader());
      assertTrue(replacement.revocations.get() > 0, "minority leader must revoke itself");
    } finally {
      for (Node node : nodes) node.close();
    }
  }

  @Test
  void persistsTermAndSharesTransportAcrossDestinationGroups() throws Exception {
    int port = freePort();
    List<String> peers = Collections.singletonList("n1@127.0.0.1:" + port);
    Node first = start("n1", port, peers, "a");
    Node secondGroup = start("n1", port, peers, "b");
    long firstTerm;
    try {
      await(Duration.ofSeconds(3), () -> first.isLeader() && secondGroup.isLeader());
      firstTerm = first.leadership.get().getEpoch();
      assertEquals(1, secondGroup.leadership.get().getEpoch());
    } finally {
      first.close();
      secondGroup.close();
    }

    Node restarted = start("n1", port, peers, "a");
    try {
      await(Duration.ofSeconds(3), restarted::isLeader);
      assertTrue(restarted.leadership.get().getEpoch() > firstTerm);
    } finally {
      restarted.close();
    }
  }

  @Test
  void isDiscoverableThroughTheExistingLeaderElectorSpi() {
    assertTrue(
        ServiceLoader.load(LeaderElector.class).stream()
            .map(ServiceLoader.Provider::get)
            .anyMatch(elector -> "raft".equals(elector.type())));
  }

  @Test
  void persistentMetadataRejectsTwoVotesInTheSameTerm() {
    RaftMetaStore store = new RaftMetaStore(temporary, "vote-safety");
    store.save(7, "n1");
    assertThrows(IllegalStateException.class, () -> store.save(7, "n2"));
    assertEquals(7, new RaftMetaStore(temporary, "vote-safety").term());
    assertEquals("n1", new RaftMetaStore(temporary, "vote-safety").votedFor());
  }

  @Test
  void wireFrameKeepsCurrentTermSeparateFromPreVoteRound() throws IOException {
    RaftMessage sent = new RaftMessage(RaftMessage.PRE_VOTE_RESPONSE, "c", "d", "n", 8, 9, true);
    byte[] encoded = sent.encode();
    RaftMessage received = RaftMessage.decode(encoded, encoded.length);
    assertEquals(8, received.term);
    assertEquals(9, received.round);
    assertTrue(received.granted);
  }

  private Node start(String nodeId, int port, List<String> peers, String destination) {
    Map<String, Object> config = new LinkedHashMap<>();
    config.put("clusterId", "test-cluster");
    config.put("bindAddress", "127.0.0.1:" + port);
    config.put("peers", peers);
    config.put("electionTimeoutMillis", 360);
    config.put("heartbeatIntervalMillis", 60);
    config.put("rpcThreads", 2);
    RaftLeaderElector elector = new RaftLeaderElector();
    elector.initialize(
        new PluginContext(destination, nodeId, "test", temporary.resolve(nodeId)), config);
    elector.start();
    Node node = new Node(elector);
    elector.participate(destination, node);
    return node;
  }

  private static int leaders(List<Node> nodes) {
    return (int) nodes.stream().filter(Node::isLeader).count();
  }

  private static List<String> peers(int[] ports) {
    List<String> peers = new ArrayList<>();
    for (int i = 0; i < ports.length; i++) peers.add("n" + (i + 1) + "@127.0.0.1:" + ports[i]);
    return peers;
  }

  private static int freePort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void await(Duration timeout, Condition condition) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.done()) return;
      Thread.sleep(20);
    }
    fail("condition not met within " + timeout);
  }

  private interface Condition {
    boolean done();
  }

  private static final class Node implements LeadershipListener, AutoCloseable {
    final RaftLeaderElector elector;
    final AtomicReference<Leadership> leadership = new AtomicReference<>();
    final AtomicInteger revocations = new AtomicInteger();
    final AtomicBoolean open = new AtomicBoolean(true);

    Node(RaftLeaderElector elector) {
      this.elector = elector;
    }

    @Override
    public void onAcquired(Leadership acquired) {
      leadership.set(acquired);
    }

    @Override
    public void onRevoked(Leadership previous) {
      leadership.compareAndSet(previous, null);
      revocations.incrementAndGet();
    }

    boolean isLeader() {
      return open.get() && leadership.get() != null;
    }

    @Override
    public void close() {
      if (open.compareAndSet(true, false)) elector.close();
    }
  }
}
