package io.github.aicanal.server;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import io.github.aicanal.api.error.CanalException;
import io.github.aicanal.api.model.*;
import io.github.aicanal.egress.netty.TcpSubscription;
import io.github.aicanal.ingress.netty.*;
import io.github.aicanal.storage.StoredEvent;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class NettyProtocolServer implements AutoCloseable {
  public static final byte HELLO = 1,
      HELLO_ACK = 2,
      PING = 3,
      PONG = 4,
      PUBLISH = 5,
      PUBLISH_ACK = 6,
      PUBLISH_NACK = 7,
      SUBSCRIBE = 8,
      SUBSCRIBE_ACK = 9,
      FETCH = 10,
      DATA_BATCH = 11,
      ACK = 12,
      ACK_COMMITTED = 13,
      STATUS = 20,
      STATUS_ACK = 21,
      ERROR = 127;
  private final int port;
  private final EmbeddedController controller;
  private final boolean authRequired;
  private final Map<String, String> roleTokens;
  private final SslContext tls;
  private EventLoopGroup boss, workers;
  private DefaultEventExecutorGroup business;
  private Channel channel;

  public NettyProtocolServer(int port, EmbeddedController controller) {
    this(port, controller, false, Collections.emptyMap(), null);
  }

  public NettyProtocolServer(
      int port,
      EmbeddedController controller,
      boolean authRequired,
      Map<String, String> roleTokens) {
    this(port, controller, authRequired, roleTokens, null);
  }

  public NettyProtocolServer(
      int port,
      EmbeddedController controller,
      boolean authRequired,
      Map<String, String> roleTokens,
      SslContext tls) {
    this.port = port;
    this.controller = controller;
    this.authRequired = authRequired;
    this.roleTokens = Collections.unmodifiableMap(new HashMap<>(roleTokens));
    this.tls = tls;
  }

  public void start() throws InterruptedException {
    boss = new NioEventLoopGroup(1);
    workers = new NioEventLoopGroup();
    business =
        new DefaultEventExecutorGroup(Math.max(2, Runtime.getRuntime().availableProcessors()));
    ServerBootstrap b = new ServerBootstrap();
    b.group(boss, workers)
        .channel(NioServerSocketChannel.class)
        .childOption(ChannelOption.TCP_NODELAY, true)
        .childHandler(
            new ChannelInitializer<SocketChannel>() {
              protected void initChannel(SocketChannel c) {
                if (tls != null) c.pipeline().addLast("tls", tls.newHandler(c.alloc()));
                c.pipeline()
                    .addLast(
                        new ProtocolDecoder(8 * 1024 * 1024),
                        new ProtocolEncoder(),
                        new IdleStateHandler(60, 30, 0, TimeUnit.SECONDS));
                c.pipeline()
                    .addLast(
                        business,
                        "commands",
                        new CommandHandler(controller, authRequired, roleTokens));
              }
            });
    channel = b.bind(port).sync().channel();
  }

  public void close() {
    if (channel != null) channel.close().awaitUninterruptibly();
    if (business != null) business.shutdownGracefully().awaitUninterruptibly();
    if (workers != null) workers.shutdownGracefully().awaitUninterruptibly();
    if (boss != null) boss.shutdownGracefully().awaitUninterruptibly();
  }

  private enum Role {
    AGENT_PRODUCER,
    DATA_CONSUMER,
    MONITOR
  }

  private static final class CommandHandler extends SimpleChannelInboundHandler<ProtocolMessage> {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final EmbeddedController controller;
    private final boolean authRequired;
    private final Map<String, String> roleTokens;
    private Role role;
    private String clientId;
    private TcpSubscription subscription;

    CommandHandler(EmbeddedController c, boolean required, Map<String, String> tokens) {
      controller = c;
      authRequired = required;
      roleTokens = tokens;
    }

    protected void channelRead0(ChannelHandlerContext ctx, ProtocolMessage m) {
      try {
        JsonNode body =
            m.getPayload().isReadable()
                ? JSON.readTree(m.getPayload().toString(StandardCharsets.UTF_8))
                : JSON.createObjectNode();
        if (m.getType() == HELLO) {
          hello(ctx, body);
          return;
        }
        if (role == null) {
          fail(ctx, ERROR, "HELLO_REQUIRED", false);
          ctx.close();
          return;
        }
        switch (m.getType()) {
          case PING:
            reply(ctx, PONG, JSON.createObjectNode());
            break;
          case STATUS:
            reply(ctx, STATUS_ACK, JSON.valueToTree(controller.serverStatus()));
            break;
          case PUBLISH:
            require(Role.AGENT_PRODUCER);
            publish(ctx, body);
            break;
          case SUBSCRIBE:
            require(Role.DATA_CONSUMER);
            subscribe(ctx, body);
            break;
          case FETCH:
            require(Role.DATA_CONSUMER);
            fetch(ctx, body);
            break;
          case ACK:
            require(Role.DATA_CONSUMER);
            ack(ctx, body);
            break;
          default:
            fail(ctx, ERROR, "UNSUPPORTED_COMMAND", false);
        }
      } catch (CanalException e) {
        fail(ctx, m.getType() == PUBLISH ? PUBLISH_NACK : ERROR, e.getCode(), e.isRetryable());
      } catch (Exception e) {
        fail(ctx, m.getType() == PUBLISH ? PUBLISH_NACK : ERROR, "INVALID_REQUEST", false);
      } finally {
        m.getPayload().release();
      }
    }

    private void hello(ChannelHandlerContext c, JsonNode n) {
      if (role != null) throw new IllegalArgumentException("HELLO already completed");
      String candidateClient = required(n, "clientId");
      Role candidateRole = Role.valueOf(required(n, "role"));
      if (authRequired && !secureEquals(roleTokens.get(candidateRole.name()), required(n, "token")))
        throw new CanalException("AUTHENTICATION_FAILED", "invalid role credential", false);
      clientId = candidateClient;
      role = candidateRole;
      ObjectNode out = JSON.createObjectNode();
      out.put("status", "OK");
      out.put("clientId", clientId);
      reply(c, HELLO_ACK, out);
    }

    private static boolean secureEquals(String expected, String supplied) {
      if (expected == null || supplied == null) return false;
      return MessageDigest.isEqual(
          expected.getBytes(StandardCharsets.UTF_8), supplied.getBytes(StandardCharsets.UTF_8));
    }

    private void publish(ChannelHandlerContext c, JsonNode n) {
      String agent = required(n, "agentId"),
          request = required(n, "requestId"),
          destination = required(n, "destination"),
          checksum = required(n, "batchChecksum");
      if (!clientId.equals(agent))
        throw new CanalException("AGENT_ID_MISMATCH", "HELLO clientId differs from agentId", false);
      List<RawResource> records = new ArrayList<>();
      for (JsonNode r : n.path("records")) {
        Map<String, String> headers = new LinkedHashMap<>();
        r.path("headers")
            .fields()
            .forEachRemaining(e -> headers.put(e.getKey(), e.getValue().asText()));
        records.add(
            new RawResource(
                destination,
                agent,
                request,
                r.path("sourceUri").asText(""),
                required(r, "sourceKey"),
                Instant.parse(required(r, "collectedAt")),
                headers,
                Base64.getDecoder().decode(required(r, "payloadBase64"))));
      }
      AgentPublishRequest q =
          new AgentPublishRequest(
              agent,
              request,
              destination,
              n.path("protocolVersion").asInt(1),
              Instant.parse(required(n, "sentAt")),
              records,
              checksum);
      IngressAck ack = controller.publish(q);
      ObjectNode out = JSON.createObjectNode();
      out.put("requestId", ack.getRequestId());
      out.put("ingestSequence", ack.getIngestSequence());
      out.put("duplicate", ack.isDuplicate());
      reply(c, PUBLISH_ACK, out);
    }

    private void subscribe(ChannelHandlerContext c, JsonNode n) {
      subscription = controller.subscribe(required(n, "destination"), required(n, "consumerId"));
      ObjectNode out = JSON.createObjectNode();
      out.put("channelId", subscription.getChannelId());
      out.put("epoch", subscription.getEpoch());
      reply(c, SUBSCRIBE_ACK, out);
    }

    private void fetch(ChannelHandlerContext c, JsonNode n) {
      if (subscription == null) throw new IllegalArgumentException("not subscribed");
      List<StoredEvent> events =
          subscription.fetch(n.path("limit").asInt(100), n.path("maxBytes").asInt(1024 * 1024));
      ObjectNode out = JSON.createObjectNode();
      ArrayNode array = out.putArray("events");
      for (StoredEvent s : events) {
        CanalEvent e = s.getEvent();
        ObjectNode x = array.addObject();
        x.put("eventId", e.getEventId());
        x.put("destination", e.getDestination());
        x.put("offset", e.getOffset());
        x.put("sourceKey", e.getSourceKey());
        x.put("category", e.getCategory());
        x.put("schemaVersion", e.getSchemaVersion());
        x.put("checksum", e.getChecksum());
        x.put("payloadBase64", Base64.getEncoder().encodeToString(e.getPayload()));
      }
      out.put("epoch", subscription.getEpoch());
      reply(c, DATA_BATCH, out);
    }

    private void ack(ChannelHandlerContext c, JsonNode n) {
      if (subscription == null) throw new IllegalArgumentException("not subscribed");
      if (n.path("epoch").asLong() != subscription.getEpoch())
        throw new CanalException("STALE_EPOCH", "ACK epoch is stale", false);
      DeliveryCheckpoint cp = subscription.ack(n.path("ackOffset").asLong());
      ObjectNode out = JSON.createObjectNode();
      out.put("committedOffset", cp.getCommittedOffset());
      out.put("version", cp.getVersion());
      reply(c, ACK_COMMITTED, out);
    }

    private static String required(JsonNode n, String key) {
      String v = n.path(key).asText();
      if (v.isEmpty()) throw new IllegalArgumentException(key + " required");
      return v;
    }

    private void require(Role expected) {
      if (role != expected)
        throw new CanalException("ROLE_FORBIDDEN", "command forbidden for role", false);
    }

    private static void fail(ChannelHandlerContext c, byte type, String code, boolean retryable) {
      ObjectNode n = JSON.createObjectNode();
      n.put("errorCode", code);
      n.put("retryable", retryable);
      reply(c, type, n);
    }

    private static void reply(ChannelHandlerContext c, byte type, JsonNode body) {
      try {
        c.writeAndFlush(
            new ProtocolMessage(
                (byte) 1, type, (short) 0, Unpooled.wrappedBuffer(JSON.writeValueAsBytes(body))));
      } catch (Exception e) {
        c.close();
      }
    }
  }
}
