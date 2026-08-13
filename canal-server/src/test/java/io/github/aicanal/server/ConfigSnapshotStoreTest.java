package io.github.aicanal.server;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.util.Hashes;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigSnapshotStoreTest {
  @TempDir Path dir;

  @Test
  void rejectedPendingFallsBackToActiveSnapshot() {
    ConfigSnapshotStore store = new ConfigSnapshotStore(dir);
    String active = "version: active\n";
    store.savePending(active, Hashes.sha256(active.getBytes(StandardCharsets.UTF_8)), 1);
    store.promote();
    String invalid = "version: invalid\n";
    store.savePending(invalid, Hashes.sha256(invalid.getBytes(StandardCharsets.UTF_8)), 2);
    assertEquals(invalid, store.loadForStartup().orElseThrow());
    store.rejectPending("invalid config");
    assertEquals(active, store.loadActiveOrLastKnownGood().orElseThrow());
    assertFalse(store.hasPending());
  }
}
