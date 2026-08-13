package io.github.aicanal.storage;

import io.github.aicanal.api.model.*;
import java.util.List;
import java.util.Optional;

public interface EventStore extends AutoCloseable {
  IngressAck appendIngress(AgentPublishRequest request, Durability durability);

  StoredEvent appendReady(long ingestSequence, int recordIndex, CanalEvent event);

  void appendRejected(long ingestSequence, int recordIndex, String errorCode, String summary);

  List<StoredEvent> readAfter(long offset, int limit, int maxBytes);

  Optional<StoredEvent> findByEventId(String eventId);

  Optional<IngestRecord> findIngress(long ingestSequence);

  boolean isProcessed(long ingestSequence, int recordIndex);

  RecoveryPlan recover();

  DeliveryCheckpoint checkpoint(String channelId, long leaderEpoch);

  DeliveryCheckpoint commitDelivery(
      String channelId, long newOffset, long expectedVersion, long leaderEpoch);

  void flush();

  @Override
  void close();
}
