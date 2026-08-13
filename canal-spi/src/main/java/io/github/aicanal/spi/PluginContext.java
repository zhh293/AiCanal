package io.github.aicanal.spi;

import java.nio.file.Path;
import java.util.Objects;

public final class PluginContext {
  private final String destination, nodeId, configVersion;
  private final Path dataDirectory;

  public PluginContext(
      String destination, String nodeId, String configVersion, Path dataDirectory) {
    this.destination = Objects.requireNonNull(destination);
    this.nodeId = Objects.requireNonNull(nodeId);
    this.configVersion = Objects.requireNonNull(configVersion);
    this.dataDirectory = Objects.requireNonNull(dataDirectory);
  }

  public String getDestination() {
    return destination;
  }

  public String getNodeId() {
    return nodeId;
  }

  public String getConfigVersion() {
    return configVersion;
  }

  public Path getDataDirectory() {
    return dataDirectory;
  }
}
