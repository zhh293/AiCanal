package io.github.aicanal.spi;

public final class DeduplicationResult {
  private final boolean duplicate;
  private final String existingEventId;

  private DeduplicationResult(boolean duplicate, String id) {
    this.duplicate = duplicate;
    this.existingEventId = id;
  }

  public static DeduplicationResult unique() {
    return new DeduplicationResult(false, null);
  }

  public static DeduplicationResult duplicate(String id) {
    return new DeduplicationResult(true, id);
  }

  public boolean isDuplicate() {
    return duplicate;
  }

  public String getExistingEventId() {
    return existingEventId;
  }
}
