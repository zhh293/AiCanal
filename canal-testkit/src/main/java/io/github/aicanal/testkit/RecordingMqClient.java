package io.github.aicanal.testkit;

import io.github.aicanal.egress.*;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

public final class RecordingMqClient {
  private final List<MessageBatch> batches = new CopyOnWriteArrayList<>();
  private volatile boolean available = true;

  public boolean send(MessageBatch b) {
    if (!available) return false;
    batches.add(b);
    return true;
  }

  public void setAvailable(boolean v) {
    available = v;
  }

  public List<MessageBatch> batches() {
    return Collections.unmodifiableList(batches);
  }
}
