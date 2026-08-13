package io.github.aicanal.ingress.netty;

import io.netty.buffer.ByteBuf;

public final class ProtocolMessage {
  public static final short MAGIC = (short) 0xA1CA;
  private final byte version, type;
  private final short flags;
  private final ByteBuf payload;

  public ProtocolMessage(byte version, byte type, short flags, ByteBuf payload) {
    this.version = version;
    this.type = type;
    this.flags = flags;
    this.payload = payload;
  }

  public byte getVersion() {
    return version;
  }

  public byte getType() {
    return type;
  }

  public short getFlags() {
    return flags;
  }

  public ByteBuf getPayload() {
    return payload;
  }
}
