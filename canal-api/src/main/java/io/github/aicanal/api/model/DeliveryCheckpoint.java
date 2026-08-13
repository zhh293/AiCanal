package io.github.aicanal.api.model;

import java.time.Instant;

public final class DeliveryCheckpoint {
  private final String destination, channelId;
  private final long committedOffset, version, leaderEpoch;
  private final Instant updatedAt;

  public DeliveryCheckpoint(
      String destination,
      String channelId,
      long committedOffset,
      long version,
      long leaderEpoch,
      Instant updatedAt) {
    this.destination = destination;
    this.channelId = channelId;
    this.committedOffset = committedOffset;
    this.version = version;
    this.leaderEpoch = leaderEpoch;
    this.updatedAt = updatedAt;
  }

  public static DeliveryCheckpoint initial(String destination, String channelId, long epoch) {
    return new DeliveryCheckpoint(destination, channelId, 0, 0, epoch, Instant.EPOCH);
  }

  public DeliveryCheckpoint advance(long offset, long epoch) {
    return new DeliveryCheckpoint(
        destination, channelId, offset, version + 1, epoch, Instant.now());
  }

  public String getDestination() {
    return destination;
  }

  public String getChannelId() {
    return channelId;
  }

  public long getCommittedOffset() {
    return committedOffset;
  }

  public long getVersion() {
    return version;
  }

  public long getLeaderEpoch() {
    return leaderEpoch;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }
}
