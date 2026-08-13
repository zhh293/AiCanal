package io.github.aicanal.core.defaults;

import io.github.aicanal.api.model.CanalEvent;
import io.github.aicanal.spi.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class HashEventDeduplicator implements EventDeduplicator {
  private final Set<String> seen = ConcurrentHashMap.newKeySet();

  public String type() {
    return "hash-default";
  }

  public void initialize(PluginContext c, Map<String, Object> x) {}

  public DeduplicationResult check(CanalEvent e) {
    return seen.add(e.getEventId())
        ? DeduplicationResult.unique()
        : DeduplicationResult.duplicate(e.getEventId());
  }
}
