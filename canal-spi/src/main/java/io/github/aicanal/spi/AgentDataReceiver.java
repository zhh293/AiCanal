package io.github.aicanal.spi;

import io.github.aicanal.api.model.AgentPublishRequest;

public interface AgentDataReceiver extends CanalPlugin {
  ReceiveResult receive(AgentPublishRequest request, RawResourceSink sink);
}
