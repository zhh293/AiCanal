package io.github.aicanal.core;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.model.*;
import io.github.aicanal.api.util.BatchChecksums;
import io.github.aicanal.cluster.*;
import io.github.aicanal.core.defaults.*;
import io.github.aicanal.ingress.netty.NettyAgentDataReceiver;
import io.github.aicanal.spi.*;
import io.github.aicanal.storage.defaults.SegmentedWalEventStore;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class CanalInstanceTest {
  @TempDir Path dir;

  @Test
  void publishRunsWholeDurablePipeline() throws Exception {
    Map<String, Object> rules = new LinkedHashMap<>();
    rules.put("rules", Collections.singletonMap("ai", Arrays.asList("AI", "LLM")));
    DestinationConfig c =
        new DestinationConfig(
            "dest",
            true,
            new PluginConfig("netty-default", Collections.emptyMap()),
            new IngressPolicy(
                IngressPolicy.Mode.FANOUT, Collections.singleton("agent"), 10, 10000, 10000),
            new PluginConfig("html-default", Collections.emptyMap()),
            new PluginConfig("rule-default", rules),
            new PluginConfig("hash-default", Collections.emptyMap()),
            new PluginConfig("slf4j-json", Collections.emptyMap()),
            new PluginConfig("segmented-wal", Collections.emptyMap()),
            new EgressConfig(EgressType.TCP, "tcp:default", Collections.emptyMap()));
    PluginRegistry r = new PluginRegistry();
    r.register(AgentDataReceiver.class, NettyAgentDataReceiver::new);
    r.register(ResourceParser.class, HtmlResourceParser::new);
    r.register(ResourceClassifier.class, RuleResourceClassifier::new);
    r.register(EventDeduplicator.class, HashEventDeduplicator::new);
    r.register(AuditLogger.class, JsonAuditLogger::new);
    StandaloneLeaderElector elector = new StandaloneLeaderElector();
    elector.initialize(new PluginContext("dest", "node", "v1", dir), Collections.emptyMap());
    CanalInstance i =
        new CanalInstance(
            c,
            "node",
            "v1",
            dir,
            new SegmentedWalEventStore(dir, "dest", 1024 * 1024, "v1", "node"),
            r,
            elector,
            new InMemoryLeaderGuard());
    i.start();
    RawResource raw =
        new RawResource(
            "dest",
            "agent",
            "req",
            "https://x",
            "key",
            Instant.EPOCH,
            Collections.emptyMap(),
            "<title>AI</title><p>LLM news</p>".getBytes());
    AgentPublishRequest unsigned =
        new AgentPublishRequest(
            "agent", "req", "dest", 1, Instant.now(), Collections.singletonList(raw), "pending");
    AgentPublishRequest request =
        new AgentPublishRequest(
            "agent",
            "req",
            "dest",
            1,
            unsigned.getSentAt(),
            unsigned.getRecords(),
            BatchChecksums.sha256(unsigned));
    i.publish(request, Durability.SYNC);
    long end = System.currentTimeMillis() + 3000;
    while (i.store().readAfter(0, 10, 10000).isEmpty() && System.currentTimeMillis() < end)
      Thread.sleep(10);
    assertEquals("ai", i.store().readAfter(0, 10, 10000).get(0).getEvent().getCategory());
    assertTrue(i.leaderGuard().isLeader("dest", 1));
    i.close();
    assertEquals(InstanceState.TERMINATED, i.state());
  }
}
