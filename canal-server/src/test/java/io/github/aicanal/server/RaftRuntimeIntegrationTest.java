package io.github.aicanal.server;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.model.InstanceState;
import java.net.DatagramSocket;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RaftRuntimeIntegrationTest {
  @TempDir Path temporary;

  @Test
  void startsServerWithElectionOnlyRaftAndPublishesItsEpoch() throws Exception {
    int raftPort = freePort();
    String yaml =
        String.join(
                "\n",
                "namespace: local.main.default",
                "cluster:",
                "  mode: raft",
                "  raft:",
                "    clusterId: integration",
                "    bindAddress: 127.0.0.1:" + raftPort,
                "    peers: [n1@127.0.0.1:" + raftPort + "]",
                "    electionTimeoutMillis: 360",
                "    heartbeatIntervalMillis: 60",
                "server:",
                "  nodeId: n1",
                "  dataDir: '" + temporary.toString().replace('\\', '/') + "'",
                "  netty: {port: 0}",
                "destinations:",
                "  - id: resources",
                "    ingress:",
                "      allowedAgents: [agent]",
                "      maxBatchRecords: 1",
                "      maxBatchBytes: 1024",
                "      maxRecordBytes: 1024",
                "    egress: {type: TCP, channelId: 'tcp:test', tcp: {}}")
            + "\n";

    try (CanalServerRuntime runtime =
        new CanalServerRuntime(new ServerConfigLoader().parse(yaml))) {
      runtime.start();
      assertEquals(InstanceState.RUNNING, runtime.controller().instanceStatus("resources"));
      await(
          Duration.ofSeconds(3),
          () ->
              ((Number) runtime.controller().deliveryStatus("resources").get("epoch")).longValue()
                  > 0);
    }
  }

  private static int freePort() throws Exception {
    try (DatagramSocket socket = new DatagramSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static void await(Duration timeout, Condition condition) throws Exception {
    long deadline = System.nanoTime() + timeout.toNanos();
    Throwable last = null;
    while (System.nanoTime() < deadline) {
      try {
        if (condition.done()) return;
      } catch (RuntimeException e) {
        last = e;
      }
      Thread.sleep(20);
    }
    fail("condition not met within " + timeout, last);
  }

  private interface Condition {
    boolean done();
  }
}
