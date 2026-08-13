package io.github.aicanal.server;

import io.github.aicanal.api.model.DestinationConfig;
import java.nio.file.Path;
import java.util.*;

public final class ServerConfig {
  private final String namespace, nodeId, clusterMode, configVersion;
  private final Path dataDir;
  private final String zkConnect, zkNamespace;
  private final Map<String, Object> raftConfig;
  private final int port, healthPort;
  private final boolean tcpAuthRequired, tlsClientAuthRequired;
  private final Map<String, String> tcpRoleTokens;
  private final Path tlsCertificate, tlsPrivateKey, tlsTrustCertificate;
  private final List<DestinationConfig> destinations;

  public ServerConfig(
      String namespace,
      String nodeId,
      String clusterMode,
      String configVersion,
      Path dataDir,
      String zkConnect,
      String zkNamespace,
      Map<String, Object> raftConfig,
      int port,
      int healthPort,
      boolean tcpAuthRequired,
      Map<String, String> tcpRoleTokens,
      Path tlsCertificate,
      Path tlsPrivateKey,
      Path tlsTrustCertificate,
      boolean tlsClientAuthRequired,
      List<DestinationConfig> destinations) {
    this.namespace = namespace;
    this.nodeId = nodeId;
    this.clusterMode = clusterMode;
    this.configVersion = configVersion;
    this.dataDir = dataDir;
    this.zkConnect = zkConnect;
    this.zkNamespace = zkNamespace;
    this.raftConfig = Collections.unmodifiableMap(new LinkedHashMap<>(raftConfig));
    this.port = port;
    this.healthPort = healthPort;
    this.tcpAuthRequired = tcpAuthRequired;
    this.tcpRoleTokens = Collections.unmodifiableMap(new LinkedHashMap<>(tcpRoleTokens));
    this.tlsCertificate = tlsCertificate;
    this.tlsPrivateKey = tlsPrivateKey;
    this.tlsTrustCertificate = tlsTrustCertificate;
    this.tlsClientAuthRequired = tlsClientAuthRequired;
    this.destinations = Collections.unmodifiableList(new ArrayList<>(destinations));
  }

  public String getNamespace() {
    return namespace;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getClusterMode() {
    return clusterMode;
  }

  public String getConfigVersion() {
    return configVersion;
  }

  public Path getDataDir() {
    return dataDir;
  }

  public String getZkConnect() {
    return zkConnect;
  }

  public String getZkNamespace() {
    return zkNamespace;
  }

  public Map<String, Object> getRaftConfig() {
    return raftConfig;
  }

  public int getPort() {
    return port;
  }

  public int getHealthPort() {
    return healthPort;
  }

  public boolean isTcpAuthRequired() {
    return tcpAuthRequired;
  }

  public Map<String, String> getTcpRoleTokens() {
    return tcpRoleTokens;
  }

  public Path getTlsCertificate() {
    return tlsCertificate;
  }

  public Path getTlsPrivateKey() {
    return tlsPrivateKey;
  }

  public Path getTlsTrustCertificate() {
    return tlsTrustCertificate;
  }

  public boolean isTlsClientAuthRequired() {
    return tlsClientAuthRequired;
  }

  public boolean isTlsEnabled() {
    return tlsCertificate != null;
  }

  public List<DestinationConfig> getDestinations() {
    return destinations;
  }
}
