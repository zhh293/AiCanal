package io.github.aicanal.api.util;

import io.github.aicanal.api.model.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.*;

public final class BatchChecksums {
  private BatchChecksums() {}

  public static String sha256(AgentPublishRequest request) {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-256");
      update(md, request.getAgentId());
      update(md, request.getRequestId());
      update(md, request.getDestination());
      for (RawResource r : request.getRecords()) {
        update(md, r.getSourceUri());
        update(md, r.getSourceKey());
        update(md, r.getCollectedAt().toString());
        r.getHeaders().entrySet().stream()
            .sorted(java.util.Map.Entry.comparingByKey())
            .forEach(
                e -> {
                  update(md, e.getKey());
                  update(md, e.getValue());
                });
        byte[] p = r.getPayload();
        md.update(ByteBuffer.allocate(4).putInt(p.length).array());
        md.update(p);
      }
      byte[] d = md.digest();
      StringBuilder s = new StringBuilder(64);
      for (byte b : d) s.append(String.format("%02x", b));
      return s.toString();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void update(MessageDigest md, String value) {
    byte[] b = value.getBytes(StandardCharsets.UTF_8);
    md.update(ByteBuffer.allocate(4).putInt(b.length).array());
    md.update(b);
  }
}
