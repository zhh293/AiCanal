package io.github.aicanal.egress;

import io.github.aicanal.spi.CanalPlugin;
import java.time.Duration;

public interface MessageQueueProducer extends CanalPlugin {
  SendResult send(MessageBatch batch, SendContext context);

  void flush(Duration timeout);
}
