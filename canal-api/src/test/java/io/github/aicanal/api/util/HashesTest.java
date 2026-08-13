package io.github.aicanal.api.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class HashesTest {
  @Test
  void eventIdIsStableAndIdentitySensitive() {
    byte[] c = "canonical".getBytes();
    String a = Hashes.stableEventId("d", "key", c);
    assertEquals(a, Hashes.stableEventId("d", "key", c));
    assertNotEquals(a, Hashes.stableEventId("d", "other", c));
    assertNotEquals(a, Hashes.stableEventId("d", "key", "changed".getBytes()));
  }
}
