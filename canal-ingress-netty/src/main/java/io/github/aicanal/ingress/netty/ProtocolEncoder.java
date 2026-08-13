package io.github.aicanal.ingress.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.handler.codec.MessageToByteEncoder;

public final class ProtocolEncoder extends MessageToByteEncoder<ProtocolMessage> {
  protected void encode(ChannelHandlerContext c, ProtocolMessage m, ByteBuf out) {
    out.writeShort(ProtocolMessage.MAGIC)
        .writeByte(m.getVersion())
        .writeByte(m.getType())
        .writeShort(m.getFlags())
        .writeInt(m.getPayload().readableBytes())
        .writeBytes(m.getPayload(), m.getPayload().readerIndex(), m.getPayload().readableBytes());
  }
}
