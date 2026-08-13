package io.github.aicanal.cluster.raft;

import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.*;

final class RaftElectionConfig {
  final String clusterId, nodeId;
  final InetSocketAddress bindAddress;
  final Map<String, RaftPeer> peers;
  final Path dataDir;
  final int electionTimeoutMillis, heartbeatIntervalMillis, rpcThreads;

  private RaftElectionConfig(
      String clusterId,
      String nodeId,
      InetSocketAddress bindAddress,
      Map<String, RaftPeer> peers,
      Path dataDir,
      int electionTimeoutMillis,
      int heartbeatIntervalMillis,
      int rpcThreads) {
    this.clusterId = clusterId;
    this.nodeId = nodeId;
    this.bindAddress = bindAddress;
    this.peers = Collections.unmodifiableMap(peers);
    this.dataDir = dataDir;
    this.electionTimeoutMillis = electionTimeoutMillis;
    this.heartbeatIntervalMillis = heartbeatIntervalMillis;
    this.rpcThreads = rpcThreads;
  }

  static RaftElectionConfig from(String nodeId, Path dataDir, Map<String, Object> values) {
    String clusterId = string(values, "clusterId", "ai-canal");
    InetSocketAddress bind = RaftPeer.parseAddress(required(values, "bindAddress"));
    Object rawPeers = values.get("peers");
    if (!(rawPeers instanceof Collection))
      throw new IllegalArgumentException("raft peers must be a list");
    Map<String, RaftPeer> peers = new LinkedHashMap<>();
    for (Object raw : (Collection<?>) rawPeers) {
      RaftPeer peer = RaftPeer.parse(String.valueOf(raw));
      if (peers.putIfAbsent(peer.nodeId, peer) != null)
        throw new IllegalArgumentException("duplicate raft peer " + peer.nodeId);
    }
    RaftPeer local = peers.get(nodeId);
    if (local == null)
      throw new IllegalArgumentException("raft peers do not contain nodeId " + nodeId);
    if (local.address.getPort() != bind.getPort())
      throw new IllegalArgumentException("raft bindAddress port differs from local peer port");
    for (RaftPeer peer : peers.values())
      if (peer.address.isUnresolved())
        throw new IllegalArgumentException(
            "cannot resolve raft peer host " + peer.address.getHostString());
    int election = integer(values, "electionTimeoutMillis", 1500);
    int heartbeat = integer(values, "heartbeatIntervalMillis", Math.max(100, election / 5));
    int threads =
        integer(
            values,
            "rpcThreads",
            Math.max(2, Math.min(8, Runtime.getRuntime().availableProcessors())));
    if (election < 300)
      throw new IllegalArgumentException("raft electionTimeoutMillis must be >= 300");
    if (heartbeat < 25 || heartbeat * 3 >= election)
      throw new IllegalArgumentException(
          "raft heartbeatIntervalMillis must be positive and less than one third of election timeout");
    if (threads < 1 || threads > 64) throw new IllegalArgumentException("invalid raft rpcThreads");
    return new RaftElectionConfig(
        clusterId, nodeId, bind, peers, dataDir, election, heartbeat, threads);
  }

  String runtimeKey() {
    return clusterId + "@" + bindAddress.getHostString() + ":" + bindAddress.getPort();
  }

  String fingerprint() {
    return nodeId
        + '|'
        + peers.values()
        + '|'
        + dataDir.toAbsolutePath().normalize()
        + '|'
        + electionTimeoutMillis
        + '|'
        + heartbeatIntervalMillis
        + '|'
        + rpcThreads;
  }

  private static String required(Map<String, Object> values, String key) {
    String value = string(values, key, "");
    if (value.isEmpty()) throw new IllegalArgumentException("raft " + key + " is required");
    return value;
  }

  private static String string(Map<String, Object> values, String key, String fallback) {
    Object value = values.get(key);
    return value == null ? fallback : String.valueOf(value).trim();
  }

  private static int integer(Map<String, Object> values, String key, int fallback) {
    Object value = values.get(key);
    return value == null ? fallback : Integer.parseInt(String.valueOf(value));
  }
}
