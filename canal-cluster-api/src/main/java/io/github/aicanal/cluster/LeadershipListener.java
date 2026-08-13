package io.github.aicanal.cluster;

public interface LeadershipListener {
  void onAcquired(Leadership leadership);

  void onRevoked(Leadership previous);
}
