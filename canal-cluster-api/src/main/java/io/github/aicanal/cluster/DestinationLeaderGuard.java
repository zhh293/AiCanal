package io.github.aicanal.cluster;

public interface DestinationLeaderGuard {
  Leadership requireLeadership(String destination);

  boolean isLeader(String destination, long expectedEpoch);
}
