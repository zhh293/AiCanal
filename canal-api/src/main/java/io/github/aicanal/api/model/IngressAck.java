package io.github.aicanal.api.model;

public final class IngressAck {
  private final String requestId;
  private final long ingestSequence;
  private final boolean duplicate;

  public IngressAck(String requestId, long ingestSequence, boolean duplicate) {
    this.requestId = requestId;
    this.ingestSequence = ingestSequence;
    this.duplicate = duplicate;
  }

  public String getRequestId() {
    return requestId;
  }

  public long getIngestSequence() {
    return ingestSequence;
  }

  public boolean isDuplicate() {
    return duplicate;
  }
}
