package io.github.aicanal.api.model;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class CanalEvent {
  private final String eventId, destination, sourceKey, category, checksum;
  private final long offset;
  private final Instant occurredAt, processedAt;
  private final int schemaVersion;
  private final Map<String, String> attributes;
  private final byte[] payload;

  public CanalEvent(
      String eventId,
      String destination,
      long offset,
      String sourceKey,
      String category,
      Instant occurredAt,
      Instant processedAt,
      int schemaVersion,
      Map<String, String> attributes,
      byte[] payload,
      String checksum) {
    this.eventId = eventId;
    this.destination = destination;
    this.offset = offset;
    this.sourceKey = sourceKey;
    this.category = category;
    this.occurredAt = occurredAt;
    this.processedAt = processedAt;
    this.schemaVersion = schemaVersion;
    this.attributes =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(attributes == null ? Collections.emptyMap() : attributes));
    this.payload = Arrays.copyOf(payload, payload.length);
    this.checksum = checksum;
  }

  public CanalEvent withOffset(long value) {
    return new CanalEvent(
        eventId,
        destination,
        value,
        sourceKey,
        category,
        occurredAt,
        processedAt,
        schemaVersion,
        attributes,
        payload,
        checksum);
  }

  public String getEventId() {
    return eventId;
  }

  public String getDestination() {
    return destination;
  }

  public long getOffset() {
    return offset;
  }

  public String getSourceKey() {
    return sourceKey;
  }

  public String getCategory() {
    return category;
  }

  public Instant getOccurredAt() {
    return occurredAt;
  }

  public Instant getProcessedAt() {
    return processedAt;
  }

  public int getSchemaVersion() {
    return schemaVersion;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public byte[] getPayload() {
    return Arrays.copyOf(payload, payload.length);
  }

  public String getChecksum() {
    return checksum;
  }
}
