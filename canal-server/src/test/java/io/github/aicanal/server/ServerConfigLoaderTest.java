package io.github.aicanal.server;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ServerConfigLoaderTest {
  @Test
  void loadsDistributionShapeAndRejectsDuplicateDestinations() {
    String base = validConfig();
    ServerConfig c = new ServerConfigLoader().parse(base);
    assertEquals(1234, c.getPort());
    assertEquals(1, c.getDestinations().size());
    assertThrows(
        IllegalArgumentException.class,
        () -> new ServerConfigLoader().parse(base + duplicateDestination()));
  }

  private static String validConfig() {
    return String.join(
            "\n",
            "namespace: local.main.default",
            "cluster: {mode: standalone}",
            "server: {nodeId: n, dataDir: ./data, netty: {port: 1234}}",
            "destinations:",
            "  - id: d",
            "    ingress:",
            "      allowedAgents: [a]",
            "      maxBatchRecords: 1",
            "      maxBatchBytes: 10",
            "      maxRecordBytes: 10",
            "    egress: {type: TCP, channelId: 'tcp:c', tcp: {}}")
        + "\n";
  }

  private static String duplicateDestination() {
    return String.join(
            "\n",
            "  - id: d",
            "    ingress:",
            "      allowedAgents: [a]",
            "      maxBatchRecords: 1",
            "      maxBatchBytes: 10",
            "      maxRecordBytes: 10",
            "    egress: {type: TCP, tcp: {}}")
        + "\n";
  }
}
