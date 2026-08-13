package io.github.aicanal.server;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.github.aicanal.api.model.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

public final class ServerConfigLoader {
  private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

  public ServerConfig load(Path path) {
    try {
      return parse(Files.readString(path));
    } catch (IOException e) {
      throw new IllegalArgumentException("cannot read config " + path, e);
    }
  }

  public ServerConfig parse(String content) {
    try {
      JsonNode r = mapper.readTree(content);
      String namespace = text(r, "namespace", "local.local.default");
      JsonNode server = r.path("server"), netty = server.path("netty"), cluster = r.path("cluster");
      String node = expand(text(server, "nodeId", "local-node"));
      Path data = Path.of(text(server, "dataDir", "./data")).toAbsolutePath().normalize();
      int port = netty.path("port").asInt(11111),
          healthPort = server.path("health").path("port").asInt(0);
      boolean authRequired =
          Boolean.parseBoolean(
              System.getenv()
                  .getOrDefault(
                      "CANAL_TCP_REQUIRE_AUTH",
                      String.valueOf(
                          netty.path("authentication").path("required").asBoolean(false))));
      Map<String, String> tokens = new LinkedHashMap<>();
      putToken(tokens, "AGENT_PRODUCER", "CANAL_AGENT_TOKEN");
      putToken(tokens, "DATA_CONSUMER", "CANAL_CONSUMER_TOKEN");
      putToken(tokens, "MONITOR", "CANAL_MONITOR_TOKEN");
      if (authRequired
          && !tokens
              .keySet()
              .containsAll(Arrays.asList("AGENT_PRODUCER", "DATA_CONSUMER", "MONITOR")))
        throw new IllegalArgumentException(
            "TCP authentication requires CANAL_AGENT_TOKEN, CANAL_CONSUMER_TOKEN and CANAL_MONITOR_TOKEN");
      Path tlsCert = envPath("CANAL_TLS_CERT"),
          tlsKey = envPath("CANAL_TLS_KEY"),
          tlsTrust = envPath("CANAL_TLS_TRUST_CERT");
      boolean tlsClientAuth =
          Boolean.parseBoolean(
              System.getenv().getOrDefault("CANAL_TLS_REQUIRE_CLIENT_AUTH", "false"));
      if ((tlsCert == null) != (tlsKey == null))
        throw new IllegalArgumentException(
            "CANAL_TLS_CERT and CANAL_TLS_KEY must be configured together");
      if (tlsClientAuth && (tlsCert == null || tlsTrust == null))
        throw new IllegalArgumentException(
            "mTLS requires CANAL_TLS_CERT, CANAL_TLS_KEY and CANAL_TLS_TRUST_CERT");
      String mode = text(cluster, "mode", "standalone");
      if (!Arrays.asList("standalone", "zookeeper", "raft").contains(mode.toLowerCase(Locale.ROOT)))
        throw new IllegalArgumentException("cluster.mode must be standalone, zookeeper or raft");
      JsonNode zk = cluster.path("zookeeper");
      JsonNode raftNode = cluster.path("raft");
      Map<String, Object> raft =
          raftNode.isObject()
              ? new LinkedHashMap<>(mapper.convertValue(raftNode, Map.class))
              : new LinkedHashMap<>();
      String raftBind = System.getenv("CANAL_RAFT_BIND_ADDRESS");
      if (raftBind != null && !raftBind.trim().isEmpty()) raft.put("bindAddress", raftBind.trim());
      if ("raft".equalsIgnoreCase(mode)) validateRaft(node, raft);
      List<DestinationConfig> destinations = new ArrayList<>();
      for (JsonNode d : r.path("destinations")) {
        PluginConfig receiver = plugin(d, "receiver", "netty-default"),
            parser = plugin(d, "parser", "html-default"),
            classifier = plugin(d, "classifier", "rule-default"),
            dedup = plugin(d, "deduplicator", "hash-default"),
            logger = plugin(d, "logger", "slf4j-json"),
            storage = plugin(d, "storage", "segmented-wal");
        JsonNode in = d.path("ingress");
        Set<String> agents = new LinkedHashSet<>();
        in.path("allowedAgents").forEach(n -> agents.add(n.asText()));
        IngressPolicy policy =
            new IngressPolicy(
                IngressPolicy.Mode.valueOf(text(in, "mode", "FANOUT")),
                agents,
                in.path("maxBatchRecords").asInt(500),
                in.path("maxBatchBytes").asInt(4 * 1024 * 1024),
                in.path("maxRecordBytes").asInt(1024 * 1024));
        JsonNode eg = d.path("egress");
        EgressType type = EgressType.valueOf(text(eg, "type", null));
        JsonNode detail = eg.path(type.name().toLowerCase(Locale.ROOT));
        Map<String, Object> em = mapper.convertValue(detail, Map.class);
        String channel = text(eg, "channelId", type.name().toLowerCase(Locale.ROOT) + ":default");
        destinations.add(
            new DestinationConfig(
                text(d, "id", null),
                d.path("enabled").asBoolean(true),
                receiver,
                policy,
                parser,
                classifier,
                dedup,
                logger,
                storage,
                new EgressConfig(type, channel, em)));
      }
      Set<String> ids = new HashSet<>();
      for (DestinationConfig d : destinations)
        if (!ids.add(d.getId()))
          throw new IllegalArgumentException("duplicate destination " + d.getId());
      return new ServerConfig(
          namespace,
          node,
          mode,
          text(r, "version", "local"),
          data,
          text(zk, "connectString", ""),
          text(zk, "namespace", "ai-canal"),
          raft,
          port,
          healthPort,
          authRequired,
          tokens,
          tlsCert,
          tlsKey,
          tlsTrust,
          tlsClientAuth,
          destinations);
    } catch (Exception e) {
      throw e instanceof IllegalArgumentException
          ? (IllegalArgumentException) e
          : new IllegalArgumentException("invalid server config", e);
    }
  }

  private PluginConfig plugin(JsonNode d, String field, String fallback) {
    JsonNode p = d.path(field);
    return new PluginConfig(
        text(p, "type", fallback), mapper.convertValue(p.path("config"), Map.class));
  }

  private static String text(JsonNode n, String k, String fallback) {
    JsonNode v = n.path(k);
    if (v.isMissingNode() || v.isNull() || v.asText().isEmpty()) {
      if (fallback == null) throw new IllegalArgumentException(k + " is required");
      return fallback;
    }
    return v.asText();
  }

  private static void putToken(Map<String, String> m, String role, String env) {
    String v = System.getenv(env);
    if (v != null && !v.trim().isEmpty()) m.put(role, v);
  }

  private static void validateRaft(String nodeId, Map<String, Object> raft) {
    Object bind = raft.get("bindAddress"), peers = raft.get("peers");
    if (bind == null || String.valueOf(bind).trim().isEmpty())
      throw new IllegalArgumentException("raft bindAddress is required");
    if (!(peers instanceof Collection) || ((Collection<?>) peers).isEmpty())
      throw new IllegalArgumentException("raft peers must be a non-empty list");
    boolean local = false;
    for (Object peer : (Collection<?>) peers) {
      String value = String.valueOf(peer);
      int at = value.indexOf('@'), colon = value.lastIndexOf(':');
      if (at < 1 || colon <= at + 1 || colon == value.length() - 1)
        throw new IllegalArgumentException("raft peer must be nodeId@host:port: " + value);
      try {
        int port = Integer.parseInt(value.substring(colon + 1));
        if (port < 1 || port > 65535) throw new NumberFormatException();
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid raft peer port: " + value);
      }
      if (nodeId.equals(value.substring(0, at))) local = true;
    }
    if (!local) throw new IllegalArgumentException("raft peers do not contain nodeId " + nodeId);
  }

  private static Path envPath(String name) {
    String value = System.getenv(name);
    return value == null || value.trim().isEmpty()
        ? null
        : Path.of(value).toAbsolutePath().normalize();
  }

  private static String expand(String s) {
    if ("${HOSTNAME}".equals(s))
      return Optional.ofNullable(System.getenv("HOSTNAME")).orElse("local-node");
    return s;
  }
}
