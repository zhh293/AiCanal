package io.github.aicanal.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class RecoveryPlan {
  private final List<IngestRecord> pending;
  private final long lastSequence, lastReadyOffset;

  public RecoveryPlan(List<IngestRecord> pending, long lastSequence, long lastReadyOffset) {
    this.pending = Collections.unmodifiableList(new ArrayList<>(pending));
    this.lastSequence = lastSequence;
    this.lastReadyOffset = lastReadyOffset;
  }

  public List<IngestRecord> getPending() {
    return pending;
  }

  public long getLastSequence() {
    return lastSequence;
  }

  public long getLastReadyOffset() {
    return lastReadyOffset;
  }
}
