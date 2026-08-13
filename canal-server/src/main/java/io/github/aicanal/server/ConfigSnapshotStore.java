package io.github.aicanal.server;

import io.github.aicanal.api.util.Hashes;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public final class ConfigSnapshotStore {
  private final Path dir;

  public ConfigSnapshotStore(Path dataDir) {
    dir = dataDir.resolve("config");
  }

  public synchronized void savePending(String content, String expectedHash, long version) {
    try {
      Files.createDirectories(dir);
      String actual = Hashes.sha256(content.getBytes(StandardCharsets.UTF_8));
      if (!actual.equals(expectedHash)) throw new IllegalArgumentException("config hash mismatch");
      atomicWrite(dir.resolve("pending.json"), content);
      atomicWrite(dir.resolve("pending.meta"), "version=" + version + "\nhash=" + actual + "\n");
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public synchronized boolean hasPending() {
    return Files.exists(dir.resolve("pending.json"));
  }

  public synchronized void promote() {
    if (!hasPending()) return;
    try {
      Path pending = dir.resolve("pending.json"), active = dir.resolve("active.json");
      if (Files.exists(active)) {
        Files.copy(
            active, dir.resolve("last-known-good.json"), StandardCopyOption.REPLACE_EXISTING);
        Path activeMeta = dir.resolve("active.meta");
        if (Files.exists(activeMeta))
          Files.copy(
              activeMeta, dir.resolve("last-known-good.meta"), StandardCopyOption.REPLACE_EXISTING);
      }
      moveAtomic(pending, active);
      moveAtomic(dir.resolve("pending.meta"), dir.resolve("active.meta"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public synchronized void rejectPending(String reason) {
    if (!hasPending()) return;
    try {
      Path rejected = dir.resolve("rejected");
      Files.createDirectories(rejected);
      String id = String.valueOf(System.currentTimeMillis());
      Files.move(
          dir.resolve("pending.json"),
          rejected.resolve(id + ".json"),
          StandardCopyOption.REPLACE_EXISTING);
      Path meta = dir.resolve("pending.meta");
      if (Files.exists(meta))
        Files.move(meta, rejected.resolve(id + ".meta"), StandardCopyOption.REPLACE_EXISTING);
      atomicWrite(rejected.resolve(id + ".reason"), reason);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public Optional<String> loadForStartup() {
    return loadFirst("pending.json", "active.json", "last-known-good.json");
  }

  public Optional<String> loadActiveOrLastKnownGood() {
    return loadFirst("active.json", "last-known-good.json");
  }

  private Optional<String> loadFirst(String... names) {
    for (String n : names) {
      Path p = dir.resolve(n);
      if (Files.exists(p))
        try {
          return Optional.of(Files.readString(p, StandardCharsets.UTF_8));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
    }
    return Optional.empty();
  }

  private static void moveAtomic(Path from, Path to) throws IOException {
    try {
      Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void atomicWrite(Path target, String content) throws IOException {
    Files.createDirectories(target.getParent());
    Path tmp = Files.createTempFile(target.getParent(), target.getFileName().toString(), ".tmp");
    try (FileChannelSync out = new FileChannelSync(tmp)) {
      out.write(content);
    }
    moveAtomic(tmp, target);
  }

  private static final class FileChannelSync implements AutoCloseable {
    private final java.nio.channels.FileChannel c;

    FileChannelSync(Path p) throws IOException {
      c = java.nio.channels.FileChannel.open(p, StandardOpenOption.WRITE);
    }

    void write(String s) throws IOException {
      java.nio.ByteBuffer b = StandardCharsets.UTF_8.encode(s);
      while (b.hasRemaining()) c.write(b);
      c.force(true);
    }

    public void close() throws IOException {
      c.close();
    }
  }
}
