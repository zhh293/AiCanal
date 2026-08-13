package io.github.aicanal.storage.defaults;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.error.CanalException;
import io.github.aicanal.api.model.*;
import io.github.aicanal.storage.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class SegmentedWalEventStoreTest {
  @TempDir Path dir;

  private static AgentPublishRequest request(String checksum) {
    RawResource r =
        new RawResource(
            "dest",
            "agent",
            "req",
            "https://x",
            "key",
            Instant.EPOCH,
            Collections.emptyMap(),
            "body".getBytes());
    return new AgentPublishRequest(
        "agent", "req", "dest", 1, Instant.EPOCH, Collections.singletonList(r), checksum);
  }

  @Test
  void idempotencyRecoveryTailTruncationAndCheckpointCas() throws Exception {
    Path active;
    long validSize;
    try (SegmentedWalEventStore s =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      IngressAck a = s.appendIngress(request("sum"), Durability.SYNC);
      assertFalse(a.isDuplicate());
      assertTrue(s.appendIngress(request("sum"), Durability.SYNC).isDuplicate());
      assertThrows(
          CanalException.class, () -> s.appendIngress(request("different"), Durability.SYNC));
      CanalEvent e =
          new CanalEvent(
              "event",
              "dest",
              0,
              "key",
              "cat",
              Instant.EPOCH,
              Instant.EPOCH,
              1,
              Collections.emptyMap(),
              "body".getBytes(),
              "hash");
      StoredEvent stored = s.appendReady(a.getIngestSequence(), 0, e);
      assertEquals(2, stored.getEvent().getOffset());
      DeliveryCheckpoint cp = s.checkpoint("tcp:c", 7);
      cp = s.commitDelivery("tcp:c", 2, cp.getVersion(), 7);
      assertEquals(2, cp.getCommittedOffset());
      long version = cp.getVersion();
      assertThrows(CheckpointConflictException.class, () -> s.commitDelivery("tcp:c", 2, 0, 7));
      assertThrows(
          CheckpointConflictException.class, () -> s.commitDelivery("tcp:c", 0, version, 7));
      active = s.activePath();
      validSize = Files.size(active);
    }
    Files.write(active, new byte[] {1, 2, 3}, StandardOpenOption.APPEND);
    try (SegmentedWalEventStore recovered =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      assertEquals(validSize, Files.size(active));
      assertEquals(1, recovered.readAfter(0, 10, 1000).size());
      assertTrue(recovered.recover().getPending().isEmpty());
      assertEquals(2, recovered.checkpoint("tcp:c", 7).getCommittedOffset());
    }
  }

  @Test
  void partialMultiRecordBatchRecoversOnlyUnfinishedIndex() {
    RawResource one =
        new RawResource(
            "dest",
            "agent",
            "multi",
            "u1",
            "k1",
            Instant.EPOCH,
            Collections.emptyMap(),
            new byte[] {1});
    RawResource two =
        new RawResource(
            "dest",
            "agent",
            "multi",
            "u2",
            "k2",
            Instant.EPOCH,
            Collections.emptyMap(),
            new byte[] {2});
    AgentPublishRequest batch =
        new AgentPublishRequest(
            "agent", "multi", "dest", 1, Instant.EPOCH, Arrays.asList(one, two), "checksum");
    long ingest;
    try (SegmentedWalEventStore s =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      ingest = s.appendIngress(batch, Durability.SYNC).getIngestSequence();
      s.appendReady(
          ingest,
          0,
          new CanalEvent(
              "e1",
              "dest",
              0,
              "k1",
              "c",
              Instant.EPOCH,
              Instant.EPOCH,
              1,
              Collections.emptyMap(),
              new byte[] {1},
              "h1"));
      assertTrue(s.isProcessed(ingest, 0));
      assertFalse(s.isProcessed(ingest, 1));
      assertEquals(1, s.recover().getPending().size());
    }
    try (SegmentedWalEventStore s =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      assertTrue(s.isProcessed(ingest, 0));
      assertFalse(s.isProcessed(ingest, 1));
      assertEquals(1, s.recover().getPending().size());
      s.appendReady(
          ingest,
          1,
          new CanalEvent(
              "e2",
              "dest",
              0,
              "k2",
              "c",
              Instant.EPOCH,
              Instant.EPOCH,
              1,
              Collections.emptyMap(),
              new byte[] {2},
              "h2"));
      assertTrue(s.recover().getPending().isEmpty());
    }
  }

  @Test
  void crcCorruptionFailsClosed() throws Exception {
    Path active;
    try (SegmentedWalEventStore s =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      s.appendIngress(request("sum"), Durability.SYNC);
      active = s.activePath();
    }
    byte[] data = Files.readAllBytes(active);
    data[22] ^= 1;
    Files.write(active, data);
    assertThrows(
        CanalException.class,
        () -> new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1"));
  }

  @Test
  void sparseIndexIsCreatedAndRebuiltFromAuthoritativeLog() throws Exception {
    Path log;
    Path index;
    try (SegmentedWalEventStore s =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      long ingest = s.appendIngress(request("sum"), Durability.SYNC).getIngestSequence();
      s.appendReady(
          ingest,
          0,
          new CanalEvent(
              "event",
              "dest",
              0,
              "key",
              "c",
              Instant.EPOCH,
              Instant.EPOCH,
              1,
              Collections.emptyMap(),
              new byte[] {1},
              "h"));
      log = s.activePath();
      String name = log.getFileName().toString();
      index = log.resolveSibling(name.substring(0, name.length() - 4) + ".idx");
      assertTrue(Files.size(index) >= 24);
    }
    Files.write(index, new byte[] {9, 9, 9}, StandardOpenOption.TRUNCATE_EXISTING);
    try (SegmentedWalEventStore ignored =
        new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "n1")) {
      assertTrue(Files.size(index) >= 24);
      assertEquals(1, ignored.readAfter(0, 10, 100).size());
    }
  }
}
