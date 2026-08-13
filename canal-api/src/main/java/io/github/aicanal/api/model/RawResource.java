package io.github.aicanal.api.model;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class RawResource {
  private final String destination, agentId, requestId, sourceUri, sourceKey;
  private final Instant collectedAt;
  private final Map<String, String> headers;
  private final byte[] payload;

  public RawResource(
      String destination,
      String agentId,
      String requestId,
      String sourceUri,
      String sourceKey,
      Instant collectedAt,
      Map<String, String> headers,
      byte[] payload) {
    this.destination = text(destination, "destination");
    this.agentId = text(agentId, "agentId");
    this.requestId = text(requestId, "requestId");
    this.sourceUri = sourceUri == null ? "" : sourceUri;
    this.sourceKey = text(sourceKey, "sourceKey");
    this.collectedAt = Objects.requireNonNull(collectedAt);
    this.headers =
        Collections.unmodifiableMap(
            new LinkedHashMap<>(headers == null ? Collections.emptyMap() : headers));
    this.payload = Arrays.copyOf(Objects.requireNonNull(payload), payload.length);
  }

  private static String text(String v, String n) {
    Objects.requireNonNull(v, n);
    if (v.trim().isEmpty()) throw new IllegalArgumentException(n + " must not be blank");
    return v;
  }

  public String getDestination() {
    return destination;
  }

  public String getAgentId() {
    return agentId;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getSourceUri() {
    return sourceUri;
  }

  public String getSourceKey() {
    return sourceKey;
  }

  public Instant getCollectedAt() {
    return collectedAt;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public byte[] getPayload() {
    return Arrays.copyOf(payload, payload.length);
  }
}
