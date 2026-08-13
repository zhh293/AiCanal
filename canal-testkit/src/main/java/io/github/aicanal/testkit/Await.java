package io.github.aicanal.testkit;

import java.time.Duration;
import java.util.function.BooleanSupplier;

public final class Await {
  private Await() {}

  public static void until(Duration timeout, BooleanSupplier condition) {
    long end = System.nanoTime() + timeout.toNanos();
    while (System.nanoTime() < end) {
      if (condition.getAsBoolean()) return;
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
    throw new AssertionError("condition not met within " + timeout);
  }
}
