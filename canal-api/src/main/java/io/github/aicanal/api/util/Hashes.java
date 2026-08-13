package io.github.aicanal.api.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class Hashes {
  private Hashes() {}

  public static String sha256(byte[] bytes) {
    try {
      byte[] d = MessageDigest.getInstance("SHA-256").digest(bytes);
      StringBuilder s = new StringBuilder(64);
      for (byte b : d) s.append(String.format("%02x", b));
      return s.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  public static String stableEventId(
      String destination, String sourceKey, byte[] canonicalContent) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      md.update(destination.getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(sourceKey.getBytes(StandardCharsets.UTF_8));
      md.update((byte) 0);
      md.update(canonicalContent);
      byte[] d = md.digest();
      StringBuilder s = new StringBuilder(64);
      for (byte b : d) s.append(String.format("%02x", b));
      return s.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
