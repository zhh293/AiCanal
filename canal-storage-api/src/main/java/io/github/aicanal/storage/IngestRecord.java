package io.github.aicanal.storage;

import io.github.aicanal.api.model.AgentPublishRequest;

public final class IngestRecord {
  private final long sequence;
  private final AgentPublishRequest request;

  public IngestRecord(long sequence, AgentPublishRequest request) {
    this.sequence = sequence;
    this.request = request;
  }

  public long getSequence() {
    return sequence;
  }

  public AgentPublishRequest getRequest() {
    return request;
  }
}
