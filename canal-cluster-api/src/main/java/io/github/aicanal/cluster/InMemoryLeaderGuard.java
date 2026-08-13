package io.github.aicanal.cluster;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class InMemoryLeaderGuard implements DestinationLeaderGuard, LeadershipListener {
  private final ConcurrentMap<String, Leadership> leaders = new ConcurrentHashMap<>();

  public Leadership requireLeadership(String d) {
    Leadership l = leaders.get(d);
    if (l == null) throw new NotLeaderException(d);
    return l;
  }

  public boolean isLeader(String d, long e) {
    Leadership l = leaders.get(d);
    return l != null && l.getEpoch() == e;
  }

  public void onAcquired(Leadership l) {
    leaders.compute(
        l.getDestination(), (d, old) -> old == null || l.getEpoch() > old.getEpoch() ? l : old);
  }

  public void onRevoked(Leadership previous) {
    leaders.computeIfPresent(
        previous.getDestination(),
        (d, current) -> current.getEpoch() == previous.getEpoch() ? null : current);
  }
}
