package io.github.aicanal.egress;

public interface DeliveryDeadLetterStore extends AutoCloseable {
  void persist(MessageBatch batch, String failure, int attempts);

  default void close() {}
}
