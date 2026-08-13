package io.github.aicanal.core;

import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.github.aicanal.api.error.CanalException;
import io.github.aicanal.api.model.*;
import io.github.aicanal.api.util.Hashes;
import io.github.aicanal.cluster.*;
import io.github.aicanal.spi.*;
import io.github.aicanal.storage.*;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public final class CanalInstance implements AutoCloseable {
  private final DestinationConfig config;
  private final String nodeId, configVersion;
  private final Path dataDir;
  private final EventStore store;
  private final PluginRegistry registry;
  private final LeaderElector elector;
  private final InMemoryLeaderGuard guard;
  private final AtomicReference<InstanceState> state = new AtomicReference<>(InstanceState.NEW);
  private final List<CanalPlugin> plugins = new ArrayList<>();
  private AgentDataReceiver receiver;
  private ResourceParser parser;
  private ResourceClassifier classifier;
  private EventDeduplicator deduplicator;
  private AuditLogger audit;
  private Disruptor<ProcessingHolder> disruptor;
  private RingBuffer<ProcessingHolder> ring;
  private LeadershipHandle leadership;
  private ExecutorService executor;

  public CanalInstance(
      DestinationConfig config,
      String nodeId,
      String configVersion,
      Path dataDir,
      EventStore store,
      PluginRegistry registry,
      LeaderElector elector,
      InMemoryLeaderGuard guard) {
    this.config = Objects.requireNonNull(config);
    this.nodeId = nodeId;
    this.configVersion = configVersion;
    this.dataDir = dataDir;
    this.store = store;
    this.registry = registry;
    this.elector = elector;
    this.guard = guard;
  }

  public synchronized void start() {
    if (!state.compareAndSet(InstanceState.NEW, InstanceState.INITIALIZING))
      throw new IllegalStateException("cannot start from " + state.get());
    try {
      PluginContext ctx = new PluginContext(config.getId(), nodeId, configVersion, dataDir);
      receiver = load(AgentDataReceiver.class, config.getReceiver(), ctx);
      parser = load(ResourceParser.class, config.getParser(), ctx);
      classifier = load(ResourceClassifier.class, config.getClassifier(), ctx);
      deduplicator = load(EventDeduplicator.class, config.getDeduplicator(), ctx);
      audit = load(AuditLogger.class, config.getLogger(), ctx);
      executor =
          Executors.newSingleThreadExecutor(
              r -> {
                Thread t = new Thread(r, "canal-pipeline-" + config.getId());
                t.setDaemon(true);
                return t;
              });
      disruptor =
          new Disruptor<>(
              ProcessingHolder::new,
              1024,
              executor,
              ProducerType.MULTI,
              new BlockingWaitStrategy());
      disruptor.setDefaultExceptionHandler(
          new ExceptionHandler<ProcessingHolder>() {
            public void handleEventException(Throwable e, long s, ProcessingHolder h) {
              reject(h, e);
            }

            public void handleOnStartException(Throwable e) {
              state.set(InstanceState.FAILED);
            }

            public void handleOnShutdownException(Throwable e) {}
          });
      disruptor.handleEventsWith(this::process);
      ring = disruptor.start();
      elector.start();
      leadership = elector.participate(config.getId(), guard);
      for (IngestRecord pending : store.recover().getPending()) publishPointer(pending);
      state.set(InstanceState.RUNNING);
    } catch (Throwable e) {
      state.set(InstanceState.FAILED);
      closeResources();
      throw e instanceof RuntimeException
          ? (RuntimeException) e
          : new CanalException("INSTANCE_START_FAILED", e.getMessage(), true, e);
    }
  }

  private <T extends CanalPlugin> T load(Class<T> c, PluginConfig p, PluginContext ctx) {
    T v = registry.create(c, p.getType());
    v.validate(p.getConfig());
    v.initialize(ctx, p.getConfig());
    v.start();
    plugins.add(v);
    return v;
  }

  public IngressAck publish(AgentPublishRequest request, Durability durability) {
    ensureAccepting();
    validate(request);
    return receiver.receive(request, (q, d) -> appendAndDispatch(q, durability)).getAck();
  }

  private IngressAck appendAndDispatch(AgentPublishRequest request, Durability durability) {
    IngressAck ack = store.appendIngress(request, durability);
    if (!ack.isDuplicate())
      store.findIngress(ack.getIngestSequence()).ifPresent(this::publishPointer);
    return ack;
  }

  private void publishPointer(IngestRecord pointer) {
    List<RawResource> records = pointer.getRequest().getRecords();
    for (int index = 0; index < records.size(); index++) {
      if (store.isProcessed(pointer.getSequence(), index)) continue;
      long sequence = ring.next();
      try {
        ProcessingHolder h = ring.get(sequence);
        h.clear();
        h.ingest = pointer;
        h.recordIndex = index;
        h.raw = records.get(index);
      } finally {
        ring.publish(sequence);
      }
    }
  }

  private void process(ProcessingHolder h, long sequence, boolean endOfBatch) {
    try {
      h.parsed = parser.parse(h.raw);
      h.classification = classifier.classify(h.parsed);
      Map<String, String> attrs = new LinkedHashMap<>(h.parsed.getAttributes());
      attrs.putAll(h.classification.getAttributes());
      byte[] canonical = h.parsed.getCanonicalContent();
      String eventId = Hashes.stableEventId(config.getId(), h.raw.getSourceKey(), canonical);
      String checksum = Hashes.sha256(canonical);
      h.event =
          new CanalEvent(
              eventId,
              config.getId(),
              0,
              h.raw.getSourceKey(),
              h.classification.getCategory(),
              h.raw.getCollectedAt(),
              Instant.now(),
              1,
              attrs,
              canonical,
              checksum);
      DeduplicationResult d = deduplicator.check(h.event);
      if (!d.isDuplicate() && !store.findByEventId(eventId).isPresent())
        store.appendReady(h.ingest.getSequence(), h.recordIndex, h.event);
      else
        store.appendRejected(
            h.ingest.getSequence(), h.recordIndex, "DUPLICATE_EVENT", "duplicate " + eventId);
      audit.record(new AuditEvent(config.getId(), "pipeline", "process", "success", eventId, ""));
    } catch (Throwable e) {
      reject(h, e);
    } finally {
      h.clear();
    }
  }

  private void reject(ProcessingHolder h, Throwable e) {
    if (h != null && h.ingest != null)
      try {
        store.appendRejected(
            h.ingest.getSequence(),
            h.recordIndex,
            "PROCESSING_FAILED",
            String.valueOf(e.getMessage()));
      } catch (Throwable ignored) {
      }
    if (audit != null)
      audit.record(
          new AuditEvent(
              config.getId(),
              "pipeline",
              "process",
              "failed",
              h != null && h.event != null ? h.event.getEventId() : "",
              "PROCESSING_FAILED"));
  }

  private void validate(AgentPublishRequest q) {
    if (!config.getId().equals(q.getDestination()))
      throw new CanalException("DESTINATION_MISMATCH", "wrong destination", false);
    IngressPolicy p = config.getIngress();
    if (!p.allows(q.getAgentId()))
      throw new CanalException("AGENT_FORBIDDEN", "agent not allowed", false);
    if (q.getRecords().size() > p.getMaxBatchRecords())
      throw new CanalException("BATCH_TOO_LARGE", "too many records", false);
    int total = 0;
    for (RawResource r : q.getRecords()) {
      int n = r.getPayload().length;
      if (n > p.getMaxRecordBytes())
        throw new CanalException("RECORD_TOO_LARGE", "payload too large", false);
      total += n;
    }
    if (total > p.getMaxBatchBytes())
      throw new CanalException("BATCH_TOO_LARGE", "batch bytes exceeded", false);
  }

  private void ensureAccepting() {
    InstanceState s = state.get();
    if (s != InstanceState.RUNNING)
      throw new CanalException(
          s == InstanceState.STOPPING ? "SERVER_DRAINING" : "INSTANCE_NOT_RUNNING",
          "instance state " + s,
          true);
  }

  public void pause() {
    if (!state.compareAndSet(InstanceState.RUNNING, InstanceState.PAUSED))
      throw new IllegalStateException();
  }

  public void resume() {
    if (!state.compareAndSet(InstanceState.PAUSED, InstanceState.RUNNING))
      throw new IllegalStateException();
  }

  public InstanceState state() {
    return state.get();
  }

  public EventStore store() {
    return store;
  }

  public DestinationLeaderGuard leaderGuard() {
    return guard;
  }

  public DestinationConfig config() {
    return config;
  }

  @Override
  public synchronized void close() {
    InstanceState s = state.get();
    if (s == InstanceState.TERMINATED) return;
    if (s != InstanceState.NEW) state.set(InstanceState.STOPPING);
    closeResources();
    state.set(InstanceState.TERMINATED);
  }

  private void closeResources() {
    if (leadership != null)
      try {
        leadership.close();
      } catch (Exception ignored) {
      }
    if (disruptor != null)
      try {
        disruptor.shutdown(30, TimeUnit.SECONDS);
      } catch (Exception e) {
        disruptor.halt();
      }
    if (executor != null) executor.shutdown();
    store.flush();
    for (int i = plugins.size() - 1; i >= 0; i--)
      try {
        plugins.get(i).close();
      } catch (Exception ignored) {
      }
    try {
      elector.close();
    } catch (Exception ignored) {
    }
    store.close();
  }
}
