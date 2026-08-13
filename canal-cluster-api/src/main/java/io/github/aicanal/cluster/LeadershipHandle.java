package io.github.aicanal.cluster;

public interface LeadershipHandle extends AutoCloseable {
  @Override
  void close();
}
