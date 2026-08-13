package io.github.aicanal.api.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class PluginConfig {
  private final String type;
  private final Map<String, Object> config;

  public PluginConfig(String type, Map<String, Object> config) {
    this.type = requireText(type, "plugin type");
    this.config =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(config == null ? Collections.emptyMap() : config));
  }

  public String getType() {
    return type;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.trim().isEmpty()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
  }
}
