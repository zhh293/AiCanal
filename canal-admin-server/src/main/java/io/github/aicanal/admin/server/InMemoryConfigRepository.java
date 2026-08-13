package io.github.aicanal.admin.server;

import io.github.aicanal.admin.api.ConfigRelease;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public final class InMemoryConfigRepository implements ConfigRepository {
  private final Map<String, NavigableMap<Long, ConfigRelease>> data = new ConcurrentHashMap<>();
  private final List<String> audit = Collections.synchronizedList(new ArrayList<>());

  public NavigableMap<Long, ConfigRelease> load(String n) {
    return data.computeIfAbsent(n, k -> new TreeMap<>());
  }

  public Set<String> namespaces() {
    return data.keySet();
  }

  public void save(ConfigRelease r) {
    data.computeIfAbsent(r.getNamespace(), k -> new TreeMap<>()).put(r.getVersion(), r);
  }

  public void audit(String a, String op, String ns, long v, String ip, String d) {
    audit.add(
        Instant.now()
            + " actor="
            + a
            + " operation="
            + op
            + " namespace="
            + ns
            + " version="
            + v
            + " sourceIp="
            + ip
            + " detail="
            + d);
  }

  public List<String> auditLog() {
    return new ArrayList<>(audit);
  }
}
