package io.github.aicanal.api.model;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class EgressConfig {
  private final EgressType type;
  private final String channelId;
  private final Map<String, Object> config;

  public EgressConfig(EgressType type, String channelId, Map<String, Object> config) {
    this.type = Objects.requireNonNull(type, "egress type");
    this.channelId = requireText(channelId, "channelId");
    this.config =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(config == null ? Collections.emptyMap() : config));
    validateRequired();
  }

  private void validateRequired() {
    switch (type) {
      case KAFKA:
        require("bootstrapServers");
        require("topic");
        break;
      case ROCKETMQ:
        require("nameServer");
        require("topic");
        break;
      case RABBITMQ:
        require("uri");
        require("exchange");
        require("routingKey");
        break;
      case TCP:
        break;
      default:
        throw new IllegalStateException("unsupported egress " + type);
    }
  }

  private void require(String key) {
    if (!config.containsKey(key) || String.valueOf(config.get(key)).trim().isEmpty())
      throw new IllegalArgumentException(type + " requires " + key);
  }

  private static String requireText(String value, String name) {
    Objects.requireNonNull(value, name);
    if (value.trim().isEmpty()) throw new IllegalArgumentException(name + " must not be blank");
    return value;
  }

  public EgressType getType() {
    return type;
  }

  public String getChannelId() {
    return channelId;
  }

  public Map<String, Object> getConfig() {
    return config;
  }
}
