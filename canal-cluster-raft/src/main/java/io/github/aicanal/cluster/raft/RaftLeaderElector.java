package io.github.aicanal.cluster.raft;

import io.github.aicanal.cluster.*;
import io.github.aicanal.spi.PluginContext;
import java.util.Map;

/** Election-only, per-destination Multi-Raft implementation of the LeaderElector SPI. */
public final class RaftLeaderElector implements LeaderElector {
  private RaftElectionConfig config;
  private RaftSharedRuntime runtime;
  private RaftElectionGroup group;

  @Override
  public String type() {
    return "raft";
  }

  @Override
  public void validate(Map<String, Object> values) {
    if (!values.containsKey("bindAddress") || !values.containsKey("peers"))
      throw new IllegalArgumentException("raft requires bindAddress and peers");
  }

  @Override
  public void initialize(PluginContext context, Map<String, Object> values) {
    validate(values);
    config = RaftElectionConfig.from(context.getNodeId(), context.getDataDirectory(), values);
  }

  @Override
  public synchronized void start() {
    if (config == null) throw new IllegalStateException("raft leader elector is not initialized");
    if (runtime == null) runtime = RaftRuntimeRegistry.acquire(config);
  }

  @Override
  public synchronized LeadershipHandle participate(
      String destination, LeadershipListener listener) {
    if (runtime == null) throw new IllegalStateException("raft leader elector is not started");
    if (group != null) throw new IllegalStateException("already participating in raft election");
    group = runtime.register(destination, listener);
    return group;
  }

  @Override
  public synchronized void close() {
    if (group != null) {
      group.close();
      group = null;
    }
    if (runtime != null) {
      RaftRuntimeRegistry.release(config, runtime);
      runtime = null;
    }
  }
}
