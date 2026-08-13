package io.github.aicanal.spi;

import io.github.aicanal.api.model.IngressAck;

public final class ReceiveResult {
  private final IngressAck ack;

  public ReceiveResult(IngressAck ack) {
    this.ack = ack;
  }

  public IngressAck getAck() {
    return ack;
  }
}
