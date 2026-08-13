package io.github.aicanal.spi;

import java.time.Instant;

public final class AuditEvent {
  private final Instant timestamp = Instant.now();
  private final String destination, component, operation, result, eventId, errorCode;

  public AuditEvent(
      String destination,
      String component,
      String operation,
      String result,
      String eventId,
      String errorCode) {
    this.destination = destination;
    this.component = component;
    this.operation = operation;
    this.result = result;
    this.eventId = eventId;
    this.errorCode = errorCode;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getDestination() {
    return destination;
  }

  public String getComponent() {
    return component;
  }

  public String getOperation() {
    return operation;
  }

  public String getResult() {
    return result;
  }

  public String getEventId() {
    return eventId;
  }

  public String getErrorCode() {
    return errorCode;
  }
}
