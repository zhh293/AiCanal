package io.github.aicanal.egress;

public final class SendContext {
  private final String destination, idempotencyKey;
  private final long leaderEpoch;

  public SendContext(String destination, long leaderEpoch, String idempotencyKey) {
    this.destination = destination;
    this.leaderEpoch = leaderEpoch;
    this.idempotencyKey = idempotencyKey;
  }

  public String getDestination() {
    return destination;
  }

  public long getLeaderEpoch() {
    return leaderEpoch;
  }

  public String getIdempotencyKey() {
    return idempotencyKey;
  }
}
