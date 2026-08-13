package io.github.aicanal.cluster;

import io.github.aicanal.spi.PluginContext;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class StandaloneLeaderElector implements LeaderElector {
  private final AtomicLong epochs = new AtomicLong();
  private String nodeId;

  public String type() {
    return "standalone";
  }

  public void initialize(PluginContext c, Map<String, Object> x) {
    nodeId = c.getNodeId();
  }

  public LeadershipHandle participate(String d, LeadershipListener l) {
    Leadership leadership = new Leadership(d, nodeId, epochs.incrementAndGet());
    l.onAcquired(leadership);
    return () -> l.onRevoked(leadership);
  }
}
