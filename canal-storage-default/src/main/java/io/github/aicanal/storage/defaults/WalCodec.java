package io.github.aicanal.storage.defaults;

import io.github.aicanal.api.model.*;
import java.io.*;
import java.time.Instant;
import java.util.*;

final class WalCodec {
  private WalCodec() {}

  static byte[] ingress(AgentPublishRequest q) {
    return write(
        out -> {
          text(out, q.getAgentId());
          text(out, q.getRequestId());
          text(out, q.getDestination());
          out.writeInt(q.getProtocolVersion());
          instant(out, q.getSentAt());
          text(out, q.getBatchChecksum());
          out.writeInt(q.getRecords().size());
          for (RawResource r : q.getRecords()) {
            text(out, r.getSourceUri());
            text(out, r.getSourceKey());
            instant(out, r.getCollectedAt());
            strings(out, r.getHeaders());
            bytes(out, r.getPayload());
          }
        });
  }

  static AgentPublishRequest ingress(byte[] b) {
    return read(
        b,
        in -> {
          String agent = text(in), request = text(in), destination = text(in);
          int protocol = in.readInt();
          Instant sent = instant(in);
          String checksum = text(in);
          int n = in.readInt();
          List<RawResource> records = new ArrayList<>();
          for (int i = 0; i < n; i++)
            records.add(
                new RawResource(
                    destination,
                    agent,
                    request,
                    text(in),
                    text(in),
                    instant(in),
                    strings(in),
                    bytes(in)));
          return new AgentPublishRequest(
              agent, request, destination, protocol, sent, records, checksum);
        });
  }

  static byte[] ready(
      long ingest, int recordIndex, CanalEvent e, String configVersion, String nodeId) {
    return write(
        out -> {
          out.writeLong(ingest);
          out.writeInt(recordIndex);
          text(out, e.getEventId());
          text(out, e.getDestination());
          out.writeLong(e.getOffset());
          text(out, e.getSourceKey());
          text(out, e.getCategory());
          instant(out, e.getOccurredAt());
          instant(out, e.getProcessedAt());
          out.writeInt(e.getSchemaVersion());
          strings(out, e.getAttributes());
          bytes(out, e.getPayload());
          text(out, e.getChecksum());
          text(out, configVersion);
          text(out, nodeId);
        });
  }

  static Ready ready(byte[] b) {
    return read(
        b,
        in -> {
          long ingest = in.readLong();
          int recordIndex = in.readInt();
          String eventId = text(in), dest = text(in);
          long offset = in.readLong();
          CanalEvent e =
              new CanalEvent(
                  eventId,
                  dest,
                  offset,
                  text(in),
                  text(in),
                  instant(in),
                  instant(in),
                  in.readInt(),
                  strings(in),
                  bytes(in),
                  text(in));
          return new Ready(ingest, recordIndex, e, text(in), text(in));
        });
  }

  static byte[] rejected(long ingest, int recordIndex, String code, String summary) {
    return write(
        o -> {
          o.writeLong(ingest);
          o.writeInt(recordIndex);
          text(o, code);
          text(o, summary);
        });
  }

  static Processed rejected(byte[] b) {
    return read(b, in -> new Processed(in.readLong(), in.readInt()));
  }

  static byte[] checkpoint(DeliveryCheckpoint c) {
    return write(
        o -> {
          text(o, c.getDestination());
          text(o, c.getChannelId());
          o.writeLong(c.getCommittedOffset());
          o.writeLong(c.getVersion());
          o.writeLong(c.getLeaderEpoch());
          instant(o, c.getUpdatedAt());
        });
  }

  static DeliveryCheckpoint checkpoint(byte[] b) {
    return read(
        b,
        i ->
            new DeliveryCheckpoint(
                text(i), text(i), i.readLong(), i.readLong(), i.readLong(), instant(i)));
  }

  static final class Processed {
    final long ingest;
    final int recordIndex;

    Processed(long i, int r) {
      ingest = i;
      recordIndex = r;
    }
  }

  static final class Ready {
    final long ingest;
    final int recordIndex;
    final CanalEvent event;
    final String configVersion, nodeId;

    Ready(long i, int r, CanalEvent e, String c, String n) {
      ingest = i;
      recordIndex = r;
      event = e;
      configVersion = c;
      nodeId = n;
    }
  }

  private interface Writer {
    void accept(DataOutputStream o) throws IOException;
  }

  private interface Reader<T> {
    T apply(DataInputStream i) throws IOException;
  }

  private static byte[] write(Writer w) {
    try {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      DataOutputStream o = new DataOutputStream(b);
      w.accept(o);
      o.flush();
      return b.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static <T> T read(byte[] b, Reader<T> r) {
    try (DataInputStream i = new DataInputStream(new ByteArrayInputStream(b))) {
      T v = r.apply(i);
      if (i.available() != 0) throw new IOException("trailing payload bytes");
      return v;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void text(DataOutputStream o, String s) throws IOException {
    o.writeUTF(s == null ? "" : s);
  }

  private static String text(DataInputStream i) throws IOException {
    return i.readUTF();
  }

  private static void instant(DataOutputStream o, Instant t) throws IOException {
    o.writeLong(t.getEpochSecond());
    o.writeInt(t.getNano());
  }

  private static Instant instant(DataInputStream i) throws IOException {
    return Instant.ofEpochSecond(i.readLong(), i.readInt());
  }

  private static void bytes(DataOutputStream o, byte[] b) throws IOException {
    o.writeInt(b.length);
    o.write(b);
  }

  private static byte[] bytes(DataInputStream i) throws IOException {
    int n = i.readInt();
    if (n < 0 || n > 64 * 1024 * 1024) throw new IOException("invalid byte array length");
    byte[] b = new byte[n];
    i.readFully(b);
    return b;
  }

  private static void strings(DataOutputStream o, Map<String, String> m) throws IOException {
    o.writeInt(m.size());
    for (Map.Entry<String, String> e : m.entrySet()) {
      text(o, e.getKey());
      text(o, e.getValue());
    }
  }

  private static Map<String, String> strings(DataInputStream i) throws IOException {
    int n = i.readInt();
    if (n < 0 || n > 10000) throw new IOException("invalid map size");
    Map<String, String> m = new LinkedHashMap<>();
    for (int x = 0; x < n; x++) m.put(text(i), text(i));
    return m;
  }
}
