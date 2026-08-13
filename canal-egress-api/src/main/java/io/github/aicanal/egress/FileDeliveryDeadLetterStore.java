package io.github.aicanal.egress;

import static java.nio.file.StandardOpenOption.*;

import io.github.aicanal.storage.StoredEvent;
import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Base64;

public final class FileDeliveryDeadLetterStore implements DeliveryDeadLetterStore {
  private final FileChannel channel;

  public FileDeliveryDeadLetterStore(Path dataDir, String destination, String channelId) {
    try {
      Path dir = dataDir.resolve(destination).resolve("dead-letter");
      Files.createDirectories(dir);
      String safe = channelId.replaceAll("[^a-zA-Z0-9._-]", "_");
      channel = FileChannel.open(dir.resolve(safe + ".jsonl"), CREATE, WRITE, APPEND);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public synchronized void persist(MessageBatch batch, String failure, int attempts) {
    StringBuilder b = new StringBuilder();
    b.append("{\"timestamp\":\"")
        .append(Instant.now())
        .append("\",\"attempts\":")
        .append(attempts)
        .append(",\"failure\":\"")
        .append(escape(failure))
        .append("\",\"fromOffset\":")
        .append(batch.getEvents().get(0).getEvent().getOffset())
        .append(",\"toOffset\":")
        .append(batch.lastOffset())
        .append(",\"events\":[");
    for (StoredEvent stored : batch.getEvents()) {
      if (b.charAt(b.length() - 1) == '}') b.append(',');
      b.append("{\"eventId\":\"")
          .append(escape(stored.getEvent().getEventId()))
          .append("\",\"offset\":")
          .append(stored.getEvent().getOffset())
          .append(",\"payloadBase64\":\"")
          .append(Base64.getEncoder().encodeToString(stored.getEvent().getPayload()))
          .append("\"}");
    }
    b.append("]}\n");
    try {
      ByteBuffer bytes = StandardCharsets.UTF_8.encode(b.toString());
      while (bytes.hasRemaining()) channel.write(bytes);
      channel.force(true);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String escape(String s) {
    return String.valueOf(s)
        .replace("\\", "\\\\")
        .replace("\"", "\\\"")
        .replace("\r", "\\r")
        .replace("\n", "\\n");
  }

  public synchronized void close() {
    try {
      channel.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
