package io.github.aicanal.api.model;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public final class IngressPolicy {
  public enum Mode {
    FANOUT,
    SHARED_STORE,
    SERVER_REPLICATION
  }

  private final Mode mode;
  private final Set<String> allowedAgents;
  private final int maxBatchRecords;
  private final int maxBatchBytes;
  private final int maxRecordBytes;

  public IngressPolicy(
      Mode mode,
      Set<String> allowedAgents,
      int maxBatchRecords,
      int maxBatchBytes,
      int maxRecordBytes) {
    this.mode = mode == null ? Mode.FANOUT : mode;
    this.allowedAgents =
        Collections.unmodifiableSet(
            new LinkedHashSet<>(allowedAgents == null ? Collections.emptySet() : allowedAgents));
    if (maxBatchRecords < 1
        || maxBatchBytes < 1
        || maxRecordBytes < 1
        || maxRecordBytes > maxBatchBytes)
      throw new IllegalArgumentException("invalid ingress limits");
    this.maxBatchRecords = maxBatchRecords;
    this.maxBatchBytes = maxBatchBytes;
    this.maxRecordBytes = maxRecordBytes;
  }

  public Mode getMode() {
    return mode;
  }

  public Set<String> getAllowedAgents() {
    return allowedAgents;
  }

  public int getMaxBatchRecords() {
    return maxBatchRecords;
  }

  public int getMaxBatchBytes() {
    return maxBatchBytes;
  }

  public int getMaxRecordBytes() {
    return maxRecordBytes;
  }

  public boolean allows(String agentId) {
    return allowedAgents.contains(agentId);
  }
}
