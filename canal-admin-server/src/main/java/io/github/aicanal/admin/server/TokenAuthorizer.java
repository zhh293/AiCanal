package io.github.aicanal.admin.server;

import com.sun.net.httpserver.HttpExchange;
import java.nio.charset.StandardCharsets;
import java.security.*;
import java.util.*;

public final class TokenAuthorizer {
  public enum Role {
    VIEWER,
    EDITOR,
    PUBLISHER,
    ADMIN
  }

  private final Map<String, Role> hashes = new HashMap<>();
  private final String machineHash;

  public TokenAuthorizer(Map<String, Role> tokens, String machineToken) {
    tokens.forEach((token, role) -> hashes.put(hash(token), role));
    machineHash = hash(machineToken);
  }

  public Role require(HttpExchange e, Role... allowed) {
    String auth = e.getRequestHeaders().getFirst("Authorization");
    if (auth == null || !auth.startsWith("Bearer "))
      throw new SecurityException("missing bearer token");
    String presented = hash(auth.substring(7));
    Role role =
        hashes.entrySet().stream()
            .filter(
                x ->
                    MessageDigest.isEqual(
                        x.getKey().getBytes(StandardCharsets.US_ASCII),
                        presented.getBytes(StandardCharsets.US_ASCII)))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new SecurityException("invalid bearer token"));
    if (Arrays.stream(allowed).noneMatch(r -> r == role) && role != Role.ADMIN)
      throw new SecurityException("role forbidden");
    return role;
  }

  public void requireMachine(HttpExchange e) {
    String auth = e.getRequestHeaders().getFirst("Authorization");
    String token = auth != null && auth.startsWith("Bearer ") ? auth.substring(7) : "";
    if (!MessageDigest.isEqual(
        machineHash.getBytes(StandardCharsets.US_ASCII),
        hash(token).getBytes(StandardCharsets.US_ASCII)))
      throw new SecurityException("invalid machine credential");
  }

  private static String hash(String token) {
    try {
      byte[] d =
          MessageDigest.getInstance("SHA-256")
              .digest(Objects.requireNonNull(token).getBytes(StandardCharsets.UTF_8));
      return Base64.getEncoder().encodeToString(d);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
