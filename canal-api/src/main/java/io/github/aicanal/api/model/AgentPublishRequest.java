package io.github.aicanal.api.model;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public final class AgentPublishRequest {
  private final String agentId, requestId, destination, batchChecksum;
  private final int protocolVersion;
  private final Instant sentAt;
  private final List<RawResource> records;

  public AgentPublishRequest(
      String agentId,
      String requestId,
      String destination,
      int protocolVersion,
      Instant sentAt,
      List<RawResource> records,
      String batchChecksum) {
    this.agentId = text(agentId, "agentId");
    this.requestId = text(requestId, "requestId");
    this.destination = text(destination, "destination");
    if (protocolVersion < 1) throw new IllegalArgumentException("protocolVersion must be positive");
    this.protocolVersion = protocolVersion;
    this.sentAt = Objects.requireNonNull(sentAt);
    if (records == null || records.isEmpty())
      throw new IllegalArgumentException("records must not be empty");
    for (RawResource r : records)
      if (!destination.equals(r.getDestination())
          || !agentId.equals(r.getAgentId())
          || !requestId.equals(r.getRequestId()))
        throw new IllegalArgumentException("record identity differs from batch");
    this.records = Collections.unmodifiableList(new ArrayList<>(records));
    this.batchChecksum = text(batchChecksum, "batchChecksum");
  }

  private static String text(String v, String n) {
    Objects.requireNonNull(v, n);
    if (v.trim().isEmpty()) throw new IllegalArgumentException(n + " must not be blank");
    return v;
  }

  public String idempotencyKey() {
    return agentId + '\u0000' + destination + '\u0000' + requestId;
  }

  public String getAgentId() {
    return agentId;
  }

  public String getRequestId() {
    return requestId;
  }

  public String getDestination() {
    return destination;
  }

  public int getProtocolVersion() {
    return protocolVersion;
  }

  public Instant getSentAt() {
    return sentAt;
  }

  public List<RawResource> getRecords() {
    return records;
  }

  public String getBatchChecksum() {
    return batchChecksum;
  }
}
