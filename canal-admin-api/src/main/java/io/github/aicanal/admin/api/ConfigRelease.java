package io.github.aicanal.admin.api;

import java.time.Instant;
import java.util.Objects;

public final class ConfigRelease {
  private final String namespace, contentHash, content, createdBy, publishedBy, comment;
  private final long version;
  private final ReleaseStatus status;
  private final Instant createdAt, publishedAt;

  public ConfigRelease(
      String namespace,
      long version,
      String contentHash,
      ReleaseStatus status,
      String content,
      String createdBy,
      Instant createdAt,
      String publishedBy,
      Instant publishedAt,
      String comment) {
    this.namespace = Objects.requireNonNull(namespace);
    this.version = version;
    this.contentHash = Objects.requireNonNull(contentHash);
    this.status = Objects.requireNonNull(status);
    this.content = Objects.requireNonNull(content);
    this.createdBy = createdBy;
    this.createdAt = createdAt;
    this.publishedBy = publishedBy;
    this.publishedAt = publishedAt;
    this.comment = comment;
  }

  public ConfigRelease withStatus(ReleaseStatus s, String actor, Instant at) {
    return new ConfigRelease(
        namespace, version, contentHash, s, content, createdBy, createdAt, actor, at, comment);
  }

  public String getNamespace() {
    return namespace;
  }

  public long getVersion() {
    return version;
  }

  public String getContentHash() {
    return contentHash;
  }

  public ReleaseStatus getStatus() {
    return status;
  }

  public String getContent() {
    return content;
  }

  public String getCreatedBy() {
    return createdBy;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public String getPublishedBy() {
    return publishedBy;
  }

  public Instant getPublishedAt() {
    return publishedAt;
  }

  public String getComment() {
    return comment;
  }
}
