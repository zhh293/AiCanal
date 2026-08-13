package io.github.aicanal.server;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import io.github.aicanal.api.model.*;
import io.github.aicanal.api.util.BatchChecksums;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class NettyDataPlaneTest {
  @TempDir Path dir;
  private CanalServerRuntime runtime;
  private NettyProtocolServer protocol;

  @AfterEach
  void close() {
    if (runtime != null) runtime.close();
    if (protocol != null) protocol.close();
  }

  @Test
  void agentPublishThenConsumerFetchAndAck() throws Exception {
    int port = freePort();
    String yaml = serverConfig(port);
    runtime = new CanalServerRuntime(new ServerConfigLoader().parse(yaml));
    runtime.start();
    RawResource raw =
        new RawResource(
            "destination",
            "agent",
            "request",
            "https://x",
            "key",
            Instant.EPOCH,
            Collections.emptyMap(),
            "<title>AI</title>news".getBytes(StandardCharsets.UTF_8));
    AgentPublishRequest unsigned =
        new AgentPublishRequest(
            "agent",
            "request",
            "destination",
            1,
            Instant.now(),
            Collections.singletonList(raw),
            "pending");
    String checksum = BatchChecksums.sha256(unsigned);
    ObjectMapper json = new ObjectMapper();
    try (Client agent = new Client(port);
        Client consumer = new Client(port)) {
      assertEquals(
          NettyProtocolServer.HELLO_ACK,
          agent.call(
                  NettyProtocolServer.HELLO,
                  json.readTree("{\"clientId\":\"agent\",\"role\":\"AGENT_PRODUCER\"}"))
              .type);
      ObjectNode publish = json.createObjectNode();
      publish.put("agentId", "agent");
      publish.put("requestId", "request");
      publish.put("destination", "destination");
      publish.put("protocolVersion", 1);
      publish.put("sentAt", unsigned.getSentAt().toString());
      publish.put("batchChecksum", checksum);
      ObjectNode record = publish.putArray("records").addObject();
      record.put("sourceUri", "https://x");
      record.put("sourceKey", "key");
      record.put("collectedAt", Instant.EPOCH.toString());
      record.putObject("headers");
      record.put("payloadBase64", Base64.getEncoder().encodeToString(raw.getPayload()));
      assertEquals(
          NettyProtocolServer.PUBLISH_ACK, agent.call(NettyProtocolServer.PUBLISH, publish).type);
      assertEquals(
          NettyProtocolServer.HELLO_ACK,
          consumer.call(
                  NettyProtocolServer.HELLO,
                  json.readTree("{\"clientId\":\"consumer\",\"role\":\"DATA_CONSUMER\"}"))
              .type);
      Reply subscribed =
          consumer.call(
              NettyProtocolServer.SUBSCRIBE,
              json.readTree("{\"destination\":\"destination\",\"consumerId\":\"consumer\"}"));
      assertEquals(NettyProtocolServer.SUBSCRIBE_ACK, subscribed.type);
      long epoch = subscribed.body.path("epoch").asLong();
      JsonNode data = null;
      for (int i = 0; i < 100; i++) {
        Reply fetched =
            consumer.call(
                NettyProtocolServer.FETCH, json.readTree("{\"limit\":10,\"maxBytes\":10000}"));
        assertEquals(NettyProtocolServer.DATA_BATCH, fetched.type);
        if (fetched.body.path("events").size() > 0) {
          data = fetched.body;
          break;
        }
        Thread.sleep(10);
      }
      assertNotNull(data);
      JsonNode event = data.path("events").get(0);
      assertEquals("ai", event.path("category").asText());
      long offset = event.path("offset").asLong();
      assertTrue(offset > 1);
      ObjectNode ack = json.createObjectNode();
      ack.put("epoch", epoch);
      ack.put("ackOffset", offset);
      Reply committed = consumer.call(NettyProtocolServer.ACK, ack);
      assertEquals(NettyProtocolServer.ACK_COMMITTED, committed.type);
      assertEquals(offset, committed.body.path("committedOffset").asLong());
    }
  }

  @Test
  void helloRejectsWrongRoleTokenAndAcceptsCorrectOne() throws Exception {
    int port = freePort();
    protocol =
        new NettyProtocolServer(
            port,
            new EmptyController(),
            true,
            Collections.singletonMap("AGENT_PRODUCER", "correct-secret"));
    protocol.start();
    ObjectMapper json = new ObjectMapper();
    try (Client client = new Client(port)) {
      Reply denied =
          client.call(
              NettyProtocolServer.HELLO,
              json.readTree(
                  "{\"clientId\":\"agent\",\"role\":\"AGENT_PRODUCER\",\"token\":\"wrong\"}"));
      assertEquals(NettyProtocolServer.ERROR, denied.type);
      assertEquals("AUTHENTICATION_FAILED", denied.body.path("errorCode").asText());
      Reply accepted =
          client.call(
              NettyProtocolServer.HELLO,
              json.readTree(
                  "{\"clientId\":\"agent\",\"role\":\"AGENT_PRODUCER\",\"token\":\"correct-secret\"}"));
      assertEquals(NettyProtocolServer.HELLO_ACK, accepted.type);
    }
  }

  private static int freePort() throws IOException {
    try (ServerSocket free = new ServerSocket(0)) {
      return free.getLocalPort();
    }
  }

  private String serverConfig(int port) {
    return String.join(
            "\n",
            "namespace: local.main.default",
            "version: test",
            "cluster: {mode: standalone}",
            "server:",
            "  nodeId: node",
            "  dataDir: '" + dir.toString().replace("\\", "/") + "'",
            "  netty: {port: " + port + "}",
            "destinations:",
            "  - id: destination",
            "    ingress:",
            "      mode: FANOUT",
            "      allowedAgents: [agent]",
            "      maxBatchRecords: 10",
            "      maxBatchBytes: 10000",
            "      maxRecordBytes: 10000",
            "    receiver: {type: netty-default, config: {requireBatchChecksum: true}}",
            "    parser: {type: html-default, config: {}}",
            "    classifier: {type: rule-default, config: {rules: {ai: [AI]}}}",
            "    deduplicator: {type: hash-default, config: {}}",
            "    logger: {type: slf4j-json, config: {}}",
            "    storage: {type: segmented-wal, config: {}}",
            "    egress: {type: TCP, channelId: 'tcp:default', tcp: {}}")
        + "\n";
  }

  private static final class EmptyController implements EmbeddedController {
    public Map<String, Object> serverStatus() {
      return Collections.singletonMap("status", "READY");
    }

    public InstanceState instanceStatus(String d) {
      return InstanceState.RUNNING;
    }

    public Map<String, InstanceState> listInstances() {
      return Collections.emptyMap();
    }

    public IngressAck publish(AgentPublishRequest r) {
      throw new UnsupportedOperationException();
    }

    public void pause(String d) {}

    public void resume(String d) {}

    public List<io.github.aicanal.storage.StoredEvent> inspectEvents(String d, long o, int l) {
      return Collections.emptyList();
    }

    public Map<String, Object> deliveryStatus(String d) {
      return Collections.emptyMap();
    }

    public io.github.aicanal.egress.netty.TcpSubscription subscribe(String d, String c) {
      throw new UnsupportedOperationException();
    }
  }

  private static final class Reply {
    final byte type;
    final JsonNode body;

    Reply(byte t, JsonNode b) {
      type = t;
      body = b;
    }
  }

  private static final class Client implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final Socket socket;
    private final DataInputStream in;
    private final DataOutputStream out;

    Client(int port) throws IOException {
      socket = new Socket("127.0.0.1", port);
      socket.setSoTimeout(5000);
      in = new DataInputStream(socket.getInputStream());
      out = new DataOutputStream(socket.getOutputStream());
    }

    Reply call(byte type, JsonNode body) throws IOException {
      byte[] payload = JSON.writeValueAsBytes(body);
      out.writeShort(0xA1CA);
      out.writeByte(1);
      out.writeByte(type);
      out.writeShort(0);
      out.writeInt(payload.length);
      out.write(payload);
      out.flush();
      assertEquals(0xA1CA, in.readUnsignedShort());
      assertEquals(1, in.readUnsignedByte());
      byte response = (byte) in.readUnsignedByte();
      in.readUnsignedShort();
      int size = in.readInt();
      byte[] bytes = new byte[size];
      in.readFully(bytes);
      return new Reply(response, JSON.readTree(bytes));
    }

    public void close() throws IOException {
      socket.close();
    }
  }
}
