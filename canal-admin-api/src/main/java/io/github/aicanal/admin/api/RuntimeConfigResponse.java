package io.github.aicanal.admin.api;

import java.time.Instant;

public final class RuntimeConfigResponse {
  private final String namespace, contentHash, config;
  private final long version;
  private final Instant publishedAt;

  public RuntimeConfigResponse(ConfigRelease r) {
    namespace = r.getNamespace();
    contentHash = r.getContentHash();
    config = r.getContent();
    version = r.getVersion();
    publishedAt = r.getPublishedAt();
  }

  public String getNamespace() {
    return namespace;
  }

  public String getContentHash() {
    return contentHash;
  }

  public String getConfig() {
    return config;
  }

  public long getVersion() {
    return version;
  }

  public Instant getPublishedAt() {
    return publishedAt;
  }
}
