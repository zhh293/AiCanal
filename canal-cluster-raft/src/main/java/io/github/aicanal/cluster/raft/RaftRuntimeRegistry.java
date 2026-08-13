package io.github.aicanal.cluster.raft;

import java.util.HashMap;
import java.util.Map;

final class RaftRuntimeRegistry {
  private static final Map<String, Entry> RUNTIMES = new HashMap<>();

  private RaftRuntimeRegistry() {}

  static synchronized RaftSharedRuntime acquire(RaftElectionConfig config) {
    Entry entry = RUNTIMES.get(config.runtimeKey());
    if (entry == null) {
      entry = new Entry(config.fingerprint(), new RaftSharedRuntime(config));
      RUNTIMES.put(config.runtimeKey(), entry);
    } else if (!entry.fingerprint.equals(config.fingerprint())) {
      throw new IllegalStateException("conflicting raft configuration for " + config.runtimeKey());
    }
    entry.references++;
    return entry.runtime;
  }

  static synchronized void release(RaftElectionConfig config, RaftSharedRuntime runtime) {
    Entry entry = RUNTIMES.get(config.runtimeKey());
    if (entry == null || entry.runtime != runtime) return;
    if (--entry.references == 0) {
      RUNTIMES.remove(config.runtimeKey());
      entry.runtime.close();
    }
  }

  private static final class Entry {
    final String fingerprint;
    final RaftSharedRuntime runtime;
    int references;

    Entry(String fingerprint, RaftSharedRuntime runtime) {
      this.fingerprint = fingerprint;
      this.runtime = runtime;
    }
  }
}
