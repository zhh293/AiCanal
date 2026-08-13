package io.github.aicanal.cluster;

import java.util.Objects;

public final class Leadership {
  private final String destination, nodeId;
  private final long epoch;

  public Leadership(String destination, String nodeId, long epoch) {
    this.destination = Objects.requireNonNull(destination);
    this.nodeId = Objects.requireNonNull(nodeId);
    if (epoch < 1) throw new IllegalArgumentException("epoch must be positive");
    this.epoch = epoch;
  }

  public String getDestination() {
    return destination;
  }

  public String getNodeId() {
    return nodeId;
  }

  public long getEpoch() {
    return epoch;
  }
}
