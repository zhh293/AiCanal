package io.github.aicanal.egress;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.model.CanalEvent;
import io.github.aicanal.storage.StoredEvent;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FileDeliveryDeadLetterStoreTest {
  @TempDir Path dir;

  @Test
  void fsyncsRecoverableEventDetailsBeforeSkip() throws Exception {
    CanalEvent event =
        new CanalEvent(
            "event-1",
            "dest",
            7,
            "key",
            "cat",
            Instant.EPOCH,
            Instant.EPOCH,
            1,
            Collections.emptyMap(),
            new byte[] {1, 2, 3},
            "hash");
    StoredEvent stored =
        new StoredEvent(event, StoredEvent.State.READY, Instant.EPOCH, null, 0, "", "v", "node");
    try (FileDeliveryDeadLetterStore store =
        new FileDeliveryDeadLetterStore(dir, "dest", "kafka:topic")) {
      store.persist(new MessageBatch(Collections.singletonList(stored)), "broker unavailable", 3);
    }
    String line = Files.readString(dir.resolve("dest/dead-letter/kafka_topic.jsonl"));
    assertTrue(line.contains("event-1"));
    assertTrue(line.contains("broker unavailable"));
    assertTrue(line.contains("AQID"));
  }
}
