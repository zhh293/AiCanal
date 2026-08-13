package io.github.aicanal.egress;

import io.github.aicanal.storage.StoredEvent;
import java.util.*;

public final class MessageBatch {
  private final List<StoredEvent> events;

  public MessageBatch(List<StoredEvent> events) {
    if (events == null || events.isEmpty()) throw new IllegalArgumentException("empty batch");
    this.events = Collections.unmodifiableList(new ArrayList<>(events));
  }

  public List<StoredEvent> getEvents() {
    return events;
  }

  public long lastOffset() {
    return events.get(events.size() - 1).getEvent().getOffset();
  }
}
