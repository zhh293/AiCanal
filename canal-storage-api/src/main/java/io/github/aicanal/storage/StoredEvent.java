package io.github.aicanal.storage;

import io.github.aicanal.api.model.CanalEvent;
import java.time.Instant;

public final class StoredEvent {
  public enum State {
    READY,
    DELIVERING,
    COMMITTED,
    DEAD_LETTER
  }

  private final CanalEvent event;
  private final State state;
  private final Instant firstWrittenAt, lastDeliveryAt;
  private final int deliveryAttempts;
  private final String lastError, configVersion, nodeId;

  public StoredEvent(
      CanalEvent event,
      State state,
      Instant firstWrittenAt,
      Instant lastDeliveryAt,
      int deliveryAttempts,
      String lastError,
      String configVersion,
      String nodeId) {
    this.event = event;
    this.state = state;
    this.firstWrittenAt = firstWrittenAt;
    this.lastDeliveryAt = lastDeliveryAt;
    this.deliveryAttempts = deliveryAttempts;
    this.lastError = lastError;
    this.configVersion = configVersion;
    this.nodeId = nodeId;
  }

  public CanalEvent getEvent() {
    return event;
  }

  public State getState() {
    return state;
  }

  public Instant getFirstWrittenAt() {
    return firstWrittenAt;
  }

  public Instant getLastDeliveryAt() {
    return lastDeliveryAt;
  }

  public int getDeliveryAttempts() {
    return deliveryAttempts;
  }

  public String getLastError() {
    return lastError;
  }

  public String getConfigVersion() {
    return configVersion;
  }

  public String getNodeId() {
    return nodeId;
  }
}
