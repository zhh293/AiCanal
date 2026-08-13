package io.github.aicanal.egress.netty;

import io.github.aicanal.api.model.DeliveryCheckpoint;
import io.github.aicanal.cluster.*;
import io.github.aicanal.storage.*;
import java.util.*;

public final class TcpSubscription {
  private final String destination, channelId;
  private final long epoch;
  private final EventStore store;
  private final DestinationLeaderGuard guard;
  private long highestSent;

  public TcpSubscription(
      String destination, String consumerId, EventStore store, DestinationLeaderGuard guard) {
    this.destination = destination;
    this.channelId = "tcp:" + consumerId;
    this.store = store;
    this.guard = guard;
    Leadership l = guard.requireLeadership(destination);
    this.epoch = l.getEpoch();
    this.highestSent = store.checkpoint(channelId, epoch).getCommittedOffset();
  }

  public synchronized List<StoredEvent> fetch(int limit, int maxBytes) {
    require();
    DeliveryCheckpoint cp = store.checkpoint(channelId, epoch);
    List<StoredEvent> batch =
        store.readAfter(Math.max(cp.getCommittedOffset(), highestSent), limit, maxBytes);
    if (!batch.isEmpty()) highestSent = batch.get(batch.size() - 1).getEvent().getOffset();
    return batch;
  }

  public synchronized DeliveryCheckpoint ack(long contiguousOffset) {
    require();
    DeliveryCheckpoint cp = store.checkpoint(channelId, epoch);
    if (contiguousOffset < cp.getCommittedOffset()) return cp;
    if (contiguousOffset > highestSent)
      throw new IllegalArgumentException("ACK exceeds sent watermark");
    return store.commitDelivery(channelId, contiguousOffset, cp.getVersion(), epoch);
  }

  private void require() {
    if (!guard.isLeader(destination, epoch)) throw new NotLeaderException(destination);
  }

  public long getEpoch() {
    return epoch;
  }

  public String getChannelId() {
    return channelId;
  }
}
