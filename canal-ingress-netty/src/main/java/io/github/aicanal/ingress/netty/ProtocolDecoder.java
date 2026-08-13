package io.github.aicanal.ingress.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;

public final class ProtocolDecoder extends ByteToMessageDecoder {
  private final int maxFrameBytes;

  public ProtocolDecoder(int maxFrameBytes) {
    this.maxFrameBytes = maxFrameBytes;
  }

  protected void decode(ChannelHandlerContext c, ByteBuf in, List<Object> out) {
    if (in.readableBytes() < 10) return;
    in.markReaderIndex();
    short magic = in.readShort();
    byte version = in.readByte(), type = in.readByte();
    short flags = in.readShort();
    int length = in.readInt();
    if (magic != ProtocolMessage.MAGIC || version != 1 || length < 0 || length > maxFrameBytes) {
      c.close();
      return;
    }
    if (in.readableBytes() < length) {
      in.resetReaderIndex();
      return;
    }
    out.add(new ProtocolMessage(version, type, flags, in.readRetainedSlice(length)));
  }
}
