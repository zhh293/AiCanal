package io.github.aicanal.admin.server;

import io.github.aicanal.admin.api.*;
import io.github.aicanal.api.util.Hashes;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public final class ConfigAdminService {
  private final ConfigValidator validator;
  private final ConfigRepository repository;

  public ConfigAdminService(ConfigValidator validator) {
    this(validator, new InMemoryConfigRepository());
  }

  public ConfigAdminService(ConfigValidator validator, ConfigRepository repository) {
    this.validator = validator;
    this.repository = repository;
  }

  public List<String> validate(String namespace, String content) {
    return validator.validate(namespace, content);
  }

  public synchronized ConfigRelease release(
      String namespace, String content, String actor, String comment) {
    List<String> errors = validator.validate(namespace, content);
    if (!errors.isEmpty()) throw new IllegalArgumentException(String.join("; ", errors));
    NavigableMap<Long, ConfigRelease> map = repository.load(namespace);
    long version = map.isEmpty() ? 1 : map.lastKey() + 1;
    String canonical = content.replace("\r\n", "\n").trim() + "\n";
    String hash = Hashes.sha256(canonical.getBytes(StandardCharsets.UTF_8));
    if (map.values().stream().anyMatch(r -> r.getContentHash().equals(hash)))
      throw new IllegalArgumentException("content already released");
    ConfigRelease r =
        new ConfigRelease(
            namespace,
            version,
            hash,
            ReleaseStatus.CREATED,
            canonical,
            actor,
            Instant.now(),
            null,
            null,
            comment);
    repository.save(r);
    repository.audit(actor, "release", namespace, version, "", "created immutable release");
    return r;
  }

  public synchronized ConfigRelease publish(String namespace, long version, String actor) {
    NavigableMap<Long, ConfigRelease> map = required(namespace);
    ConfigRelease target =
        Optional.ofNullable(map.get(version))
            .orElseThrow(() -> new IllegalArgumentException("release not found"));
    for (ConfigRelease r : map.values())
      if (r.getStatus() == ReleaseStatus.PUBLISHED)
        repository.save(r.withStatus(ReleaseStatus.SUPERSEDED, actor, Instant.now()));
    ConfigRelease published = target.withStatus(ReleaseStatus.PUBLISHED, actor, Instant.now());
    repository.save(published);
    repository.audit(actor, "publish", namespace, version, "", "published");
    return published;
  }

  public synchronized ConfigRelease rollback(
      String namespace, long version, String actor, String comment) {
    ConfigRelease old =
        Optional.ofNullable(required(namespace).get(version))
            .orElseThrow(() -> new IllegalArgumentException("release not found"));
    ConfigRelease copy =
        release(namespace, old.getContent() + "# rollback-of: " + version + "\n", actor, comment);
    return publish(namespace, copy.getVersion(), actor);
  }

  public Optional<ConfigRelease> published(String namespace) {
    return repository.load(namespace).descendingMap().values().stream()
        .filter(r -> r.getStatus() == ReleaseStatus.PUBLISHED)
        .findFirst();
  }

  public List<ConfigRelease> list(String n) {
    return Collections.unmodifiableList(new ArrayList<>(repository.load(n).values()));
  }

  public String diff(String namespace, long left, long right) {
    ConfigRelease a = requiredRelease(namespace, left), b = requiredRelease(namespace, right);
    List<String> x = Arrays.asList(a.getContent().split("\n", -1)),
        y = Arrays.asList(b.getContent().split("\n", -1));
    StringBuilder out =
        new StringBuilder("--- version-")
            .append(left)
            .append("\n+++ version-")
            .append(right)
            .append('\n');
    int max = Math.max(x.size(), y.size());
    for (int i = 0; i < max; i++) {
      String l = i < x.size() ? x.get(i) : null, r = i < y.size() ? y.get(i) : null;
      if (!Objects.equals(l, r)) {
        if (l != null) out.append('-').append(l).append('\n');
        if (r != null) out.append('+').append(r).append('\n');
      }
    }
    return out.toString();
  }

  public List<String> auditLog() {
    return repository.auditLog();
  }

  private ConfigRelease requiredRelease(String n, long v) {
    ConfigRelease r = required(n).get(v);
    if (r == null) throw new IllegalArgumentException("release not found");
    return r;
  }

  private NavigableMap<Long, ConfigRelease> required(String n) {
    NavigableMap<Long, ConfigRelease> m = repository.load(n);
    if (m.isEmpty()) throw new IllegalArgumentException("namespace not found");
    return m;
  }
}
