package io.github.aicanal.admin.server;

import io.github.aicanal.admin.api.ConfigRelease;
import java.util.*;

public interface ConfigRepository extends AutoCloseable {
  NavigableMap<Long, ConfigRelease> load(String namespace);

  Set<String> namespaces();

  void save(ConfigRelease release);

  void audit(
      String actor,
      String operation,
      String namespace,
      long version,
      String sourceIp,
      String detail);

  List<String> auditLog();

  default void close() {}
}
