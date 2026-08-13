package io.github.aicanal.server;

import io.github.aicanal.api.model.*;
import io.github.aicanal.core.CanalInstance;
import io.github.aicanal.egress.netty.TcpSubscription;
import io.github.aicanal.storage.StoredEvent;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class DefaultEmbeddedController implements EmbeddedController, AutoCloseable {
  private final Map<String, CanalInstance> instances = new ConcurrentHashMap<>();
  private final long started = System.currentTimeMillis();

  public void add(CanalInstance i) {
    if (instances.putIfAbsent(i.config().getId(), i) != null)
      throw new IllegalArgumentException("duplicate destination");
  }

  public Map<String, Object> serverStatus() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(
        "status",
        instances.values().stream().allMatch(i -> i.state() == InstanceState.RUNNING)
            ? "READY"
            : "DEGRADED");
    m.put("uptimeMillis", System.currentTimeMillis() - started);
    m.put("instances", instances.size());
    return m;
  }

  private CanalInstance require(String d) {
    CanalInstance i = instances.get(d);
    if (i == null) throw new IllegalArgumentException("unknown destination " + d);
    return i;
  }

  public InstanceState instanceStatus(String d) {
    return require(d).state();
  }

  public Map<String, InstanceState> listInstances() {
    Map<String, InstanceState> m = new TreeMap<>();
    instances.forEach((k, v) -> m.put(k, v.state()));
    return m;
  }

  public IngressAck publish(AgentPublishRequest q) {
    return require(q.getDestination()).publish(q, Durability.GROUP_SYNC);
  }

  public void pause(String d) {
    require(d).pause();
  }

  public void resume(String d) {
    require(d).resume();
  }

  public List<StoredEvent> inspectEvents(String d, long offset, int limit) {
    return require(d).store().readAfter(offset, limit, 4 * 1024 * 1024);
  }

  public Map<String, Object> deliveryStatus(String d) {
    CanalInstance i = require(d);
    long epoch = i.leaderGuard().requireLeadership(d).getEpoch();
    DeliveryCheckpoint cp = i.store().checkpoint(i.config().getEgress().getChannelId(), epoch);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("checkpoint", cp.getCommittedOffset());
    m.put("epoch", epoch);
    m.put("channel", cp.getChannelId());
    return m;
  }

  public TcpSubscription subscribe(String d, String consumerId) {
    CanalInstance i = require(d);
    if (i.config().getEgress().getType() != EgressType.TCP)
      throw new IllegalArgumentException("destination is not TCP egress");
    return new TcpSubscription(d, consumerId, i.store(), i.leaderGuard());
  }

  public void close() {
    for (CanalInstance i : instances.values()) i.close();
    instances.clear();
  }
}
