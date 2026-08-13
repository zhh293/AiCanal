package io.github.aicanal.api.model;

import java.util.Objects;

public final class Health {
  public enum Status {
    UP,
    DEGRADED,
    DOWN
  }

  private final Status status;
  private final String detail;

  private Health(Status status, String detail) {
    this.status = Objects.requireNonNull(status);
    this.detail = detail == null ? "" : detail;
  }

  public static Health up() {
    return new Health(Status.UP, "");
  }

  public static Health degraded(String detail) {
    return new Health(Status.DEGRADED, detail);
  }

  public static Health down(String detail) {
    return new Health(Status.DOWN, detail);
  }

  public Status getStatus() {
    return status;
  }

  public String getDetail() {
    return detail;
  }
}
