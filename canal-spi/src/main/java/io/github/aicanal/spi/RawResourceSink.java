package io.github.aicanal.spi;

import io.github.aicanal.api.model.*;

public interface RawResourceSink {
  IngressAck append(AgentPublishRequest request, Durability durability);
}
