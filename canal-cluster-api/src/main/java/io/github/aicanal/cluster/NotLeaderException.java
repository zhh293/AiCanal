package io.github.aicanal.cluster;

import io.github.aicanal.api.error.CanalException;

public final class NotLeaderException extends CanalException {
  public NotLeaderException(String destination) {
    super("NOT_LEADER", "node cannot prove leadership for " + destination, true);
  }
}
