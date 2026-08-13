package io.github.aicanal.cluster.raft;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Properties;

final class RaftMetaStore {
  private final Path file;
  private long term;
  private String votedFor = "";

  RaftMetaStore(Path dataDir, String groupId) {
    file = dataDir.resolve("raft-election").resolve(groupId).resolve("meta.properties");
    load();
  }

  synchronized long term() {
    return term;
  }

  synchronized String votedFor() {
    return votedFor;
  }

  synchronized void save(long newTerm, String newVotedFor) {
    if (newTerm < term) throw new IllegalArgumentException("raft term cannot move backwards");
    if (newTerm == term
        && !votedFor.isEmpty()
        && !newVotedFor.isEmpty()
        && !votedFor.equals(newVotedFor))
      throw new IllegalStateException("raft node cannot vote twice in term " + term);
    try {
      Files.createDirectories(file.getParent());
      String content = "term=" + newTerm + "\nvotedFor=" + newVotedFor + "\n";
      Path temporary = Files.createTempFile(file.getParent(), "meta", ".tmp");
      try {
        try (FileChannel channel = FileChannel.open(temporary, StandardOpenOption.WRITE)) {
          ByteBuffer buffer = StandardCharsets.UTF_8.encode(content);
          while (buffer.hasRemaining()) channel.write(buffer);
          channel.force(true);
        }
        try {
          Files.move(
              temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
          Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
        }
      } finally {
        Files.deleteIfExists(temporary);
      }
      term = newTerm;
      votedFor = newVotedFor;
    } catch (IOException e) {
      throw new UncheckedIOException("cannot persist raft election metadata", e);
    }
  }

  private void load() {
    if (!Files.exists(file)) return;
    Properties properties = new Properties();
    try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
      properties.load(reader);
      term = Long.parseLong(properties.getProperty("term", "0"));
      votedFor = properties.getProperty("votedFor", "");
      if (term < 0) throw new IOException("negative raft term");
    } catch (Exception e) {
      throw new IllegalStateException("invalid raft election metadata " + file, e);
    }
  }
}
