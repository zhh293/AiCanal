package io.github.aicanal.egress;

public final class SendResult {
  private final boolean confirmed, retryable;
  private final String detail;

  private SendResult(boolean c, boolean r, String d) {
    confirmed = c;
    retryable = r;
    detail = d;
  }

  public static SendResult confirmed() {
    return new SendResult(true, false, "");
  }

  public static SendResult failed(boolean retryable, String detail) {
    return new SendResult(false, retryable, detail);
  }

  public boolean isConfirmed() {
    return confirmed;
  }

  public boolean isRetryable() {
    return retryable;
  }

  public String getDetail() {
    return detail;
  }
}
