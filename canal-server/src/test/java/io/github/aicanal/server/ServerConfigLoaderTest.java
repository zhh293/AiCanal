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

  @Test
  void validatesRaftClusterConfigurationWithoutChangingOtherModes() {
    ServerConfig standalone = new ServerConfigLoader().parse(validConfig());
    assertEquals("standalone", standalone.getClusterMode());
    assertTrue(standalone.getRaftConfig().isEmpty());

    String raft =
        validConfig()
            .replace(
                "cluster: {mode: standalone}",
                String.join(
                    "\n",
                    "cluster:",
                    "  mode: raft",
                    "  raft:",
                    "    clusterId: test",
                    "    bindAddress: 127.0.0.1:17001",
                    "    peers: [n@127.0.0.1:17001]"));
    ServerConfig parsed = new ServerConfigLoader().parse(raft);
    assertEquals("raft", parsed.getClusterMode());
    assertEquals("127.0.0.1:17001", parsed.getRaftConfig().get("bindAddress"));

    assertThrows(
        IllegalArgumentException.class,
        () -> new ServerConfigLoader().parse(validConfig().replace("standalone", "typo")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new ServerConfigLoader().parse(raft.replace("n@127.0.0.1", "other@127.0.0.1")));
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
