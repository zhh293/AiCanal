package io.github.aicanal.spi;

import io.github.aicanal.api.model.Health;
import java.util.Map;

public interface CanalPlugin extends AutoCloseable {
  String type();

  void initialize(PluginContext context, Map<String, Object> config);

  default void validate(Map<String, Object> config) {}

  default void start() {}

  default Health health() {
    return Health.up();
  }

  @Override
  default void close() {}
}
