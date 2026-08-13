package io.github.aicanal.spi;

import io.github.aicanal.api.model.CanalEvent;

public interface EventDeduplicator extends CanalPlugin {
  DeduplicationResult check(CanalEvent event);
}
