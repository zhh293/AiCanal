package io.github.aicanal.cluster.raft;

import java.io.*;
import java.nio.charset.StandardCharsets;

final class RaftMessage {
  static final byte PRE_VOTE_REQUEST = 1,
      PRE_VOTE_RESPONSE = 2,
      VOTE_REQUEST = 3,
      VOTE_RESPONSE = 4,
      HEARTBEAT = 5,
      HEARTBEAT_RESPONSE = 6;
  private static final int MAGIC = 0x41494352;
  private static final byte VERSION = 1;

  final byte type;
  final String clusterId, groupId, senderId;
  final long term, round;
  final boolean granted;

  RaftMessage(
      byte type,
      String clusterId,
      String groupId,
      String senderId,
      long term,
      long round,
      boolean granted) {
    this.type = type;
    this.clusterId = clusterId;
    this.groupId = groupId;
    this.senderId = senderId;
    this.term = term;
    this.round = round;
    this.granted = granted;
  }

  byte[] encode() {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream(128);
      DataOutputStream out = new DataOutputStream(bytes);
      out.writeInt(MAGIC);
      out.writeByte(VERSION);
      out.writeByte(type);
      writeString(out, clusterId);
      writeString(out, groupId);
      writeString(out, senderId);
      out.writeLong(term);
      out.writeLong(round);
      out.writeBoolean(granted);
      out.flush();
      return bytes.toByteArray();
    } catch (IOException impossible) {
      throw new UncheckedIOException(impossible);
    }
  }

  static RaftMessage decode(byte[] bytes, int length) throws IOException {
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, length));
    if (in.readInt() != MAGIC || in.readByte() != VERSION)
      throw new IOException("invalid raft frame");
    byte type = in.readByte();
    if (type < PRE_VOTE_REQUEST || type > HEARTBEAT_RESPONSE)
      throw new IOException("invalid raft message type");
    return new RaftMessage(
        type,
        readString(in),
        readString(in),
        readString(in),
        in.readLong(),
        in.readLong(),
        in.readBoolean());
  }

  private static void writeString(DataOutputStream out, String value) throws IOException {
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    if (bytes.length > 1024) throw new IOException("raft string too long");
    out.writeShort(bytes.length);
    out.write(bytes);
  }

  private static String readString(DataInputStream in) throws IOException {
    int length = in.readUnsignedShort();
    if (length > 1024) throw new IOException("raft string too long");
    byte[] bytes = new byte[length];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
