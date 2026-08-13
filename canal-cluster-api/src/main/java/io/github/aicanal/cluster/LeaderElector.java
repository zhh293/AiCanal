package io.github.aicanal.cluster;

import io.github.aicanal.spi.CanalPlugin;

public interface LeaderElector extends CanalPlugin {
  LeadershipHandle participate(String destination, LeadershipListener listener);
}
