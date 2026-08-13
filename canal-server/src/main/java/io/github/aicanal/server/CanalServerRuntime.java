package io.github.aicanal.server;

import io.github.aicanal.api.model.*;
import io.github.aicanal.cluster.*;
import io.github.aicanal.cluster.zookeeper.ZookeeperLeaderElector;
import io.github.aicanal.core.*;
import io.github.aicanal.core.defaults.*;
import io.github.aicanal.egress.*;
import io.github.aicanal.egress.kafka.KafkaProducerAdapter;
import io.github.aicanal.egress.rabbitmq.RabbitMqProducerAdapter;
import io.github.aicanal.egress.rocketmq.RocketMqProducerAdapter;
import io.github.aicanal.ingress.netty.NettyAgentDataReceiver;
import io.github.aicanal.spi.*;
import io.github.aicanal.storage.defaults.SegmentedWalEventStore;
import java.time.Duration;
import java.util.*;

public final class CanalServerRuntime implements AutoCloseable {
  private final ServerConfig config;
  private final DefaultEmbeddedController controller = new DefaultEmbeddedController();
  private final List<EgressRuntime> egress = new ArrayList<>();
  private NettyProtocolServer netty;
  private HealthHttpServer health;

  public CanalServerRuntime(ServerConfig config) {
    this.config = config;
  }

  public void start() {
    PluginRegistry registry = new PluginRegistry();
    registry.register(AgentDataReceiver.class, NettyAgentDataReceiver::new);
    registry.register(ResourceParser.class, HtmlResourceParser::new);
    registry.register(ResourceClassifier.class, RuleResourceClassifier::new);
    registry.register(EventDeduplicator.class, HashEventDeduplicator::new);
    registry.register(AuditLogger.class, JsonAuditLogger::new);
    for (DestinationConfig d : config.getDestinations()) {
      if (!d.isEnabled()) continue;
      LeaderElector elector =
          "zookeeper".equalsIgnoreCase(config.getClusterMode())
              ? new ZookeeperLeaderElector()
              : new StandaloneLeaderElector();
      Map<String, Object> clusterCfg = new LinkedHashMap<>();
      if (elector instanceof ZookeeperLeaderElector) {
        clusterCfg.put("connectString", config.getZkConnect());
        clusterCfg.put("namespace", config.getZkNamespace());
      }
      PluginContext context =
          new PluginContext(
              d.getId(), config.getNodeId(), config.getConfigVersion(), config.getDataDir());
      elector.initialize(context, clusterCfg);
      SegmentedWalEventStore store =
          new SegmentedWalEventStore(
              config.getDataDir(),
              d.getId(),
              1024L * 1024 * 1024,
              config.getConfigVersion(),
              config.getNodeId());
      InMemoryLeaderGuard guard = new InMemoryLeaderGuard();
      CanalInstance instance =
          new CanalInstance(
              d,
              config.getNodeId(),
              config.getConfigVersion(),
              config.getDataDir(),
              store,
              registry,
              elector,
              guard);
      instance.start();
      controller.add(instance);
      if (d.getEgress().getType() != EgressType.TCP) {
        MessageQueueProducer producer = producer(d.getEgress().getType());
        producer.validate(d.getEgress().getConfig());
        producer.initialize(context, d.getEgress().getConfig());
        producer.start();
        Map<String, Object> cfg = d.getEgress().getConfig();
        MqDestinationWorker worker =
            new MqDestinationWorker(
                d.getId(),
                d.getEgress().getChannelId(),
                store,
                guard,
                producer,
                new FileDeliveryDeadLetterStore(
                    config.getDataDir(), d.getId(), d.getEgress().getChannelId()),
                integer(cfg, "batchSize", 200),
                integer(cfg, "maxBatchBytes", 1024 * 1024),
                integer(cfg, "maxAttempts", 8),
                Duration.ofMillis(integer(cfg, "initialBackoffMillis", 1000)),
                Duration.ofMillis(integer(cfg, "maxBackoffMillis", 300000)),
                MqDestinationWorker.DeadLetterPolicy.valueOf(
                    String.valueOf(cfg.getOrDefault("deadLetterPolicy", "BLOCK"))));
        worker.start();
        egress.add(worker);
      }
    }
    try {
      netty =
          new NettyProtocolServer(
              config.getPort(),
              controller,
              config.isTcpAuthRequired(),
              config.getTcpRoleTokens(),
              TlsSupport.build(config));
      netty.start();
      if (config.getHealthPort() > 0) {
        health = new HealthHttpServer(config.getHealthPort(), controller);
        health.start();
      }
    } catch (Exception e) {
      close();
      throw new IllegalStateException("server startup failed", e);
    }
  }

  private static MessageQueueProducer producer(EgressType type) {
    switch (type) {
      case KAFKA:
        return new KafkaProducerAdapter();
      case ROCKETMQ:
        return new RocketMqProducerAdapter();
      case RABBITMQ:
        return new RabbitMqProducerAdapter();
      default:
        throw new IllegalArgumentException("not an MQ type " + type);
    }
  }

  private static int integer(Map<String, Object> c, String key, int fallback) {
    return Integer.parseInt(String.valueOf(c.getOrDefault(key, fallback)));
  }

  public EmbeddedController controller() {
    return controller;
  }

  public void close() {
    if (health != null) {
      health.close();
      health = null;
    }
    if (netty != null) {
      netty.close();
      netty = null;
    }
    for (int i = egress.size() - 1; i >= 0; i--)
      try {
        egress.get(i).close();
      } catch (Exception ignored) {
      }
    egress.clear();
    controller.close();
  }
}
