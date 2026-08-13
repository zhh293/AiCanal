package io.github.aicanal.server;

import java.net.URI;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public final class CanalServerMain {
  private static final int EXIT_CONFIG_RESTART = 20;

  private CanalServerMain() {}

  public static void main(String[] args) throws Exception {
    Path configPath = Path.of("canal-distribution/config/application.yaml");
    for (int i = 0; i < args.length - 1; i++)
      if ("--config".equals(args[i])) configPath = Path.of(args[i + 1]);
    ServerConfigLoader loader = new ServerConfigLoader();
    ServerConfig bootstrap = loader.load(configPath);
    ConfigSnapshotStore snapshots = new ConfigSnapshotStore(bootstrap.getDataDir());
    ServerConfig selected = bootstrap;
    boolean pending = snapshots.hasPending();
    Optional<String> snapshot = snapshots.loadForStartup();
    if (snapshot.isPresent())
      try {
        selected = loader.parse(snapshot.get());
      } catch (Exception e) {
        if (pending) snapshots.rejectPending(e.toString());
        selected = snapshots.loadActiveOrLastKnownGood().map(loader::parse).orElse(bootstrap);
      }
    CanalServerRuntime runtime = new CanalServerRuntime(selected);
    try {
      runtime.start();
      if (pending) snapshots.promote();
    } catch (Exception startup) {
      if (pending) {
        snapshots.rejectPending(startup.toString());
        ServerConfig fallback =
            snapshots.loadActiveOrLastKnownGood().map(loader::parse).orElse(bootstrap);
        runtime = new CanalServerRuntime(fallback);
        runtime.start();
        selected = fallback;
      } else throw startup;
    }
    CanalServerRuntime activeRuntime = runtime;
    Runtime.getRuntime().addShutdownHook(new Thread(activeRuntime::close, "canal-shutdown"));
    Map<String, String> env = System.getenv();
    String adminBase = env.get("CANAL_ADMIN_BASE_URL"),
        machineToken = env.get("CANAL_ADMIN_MACHINE_TOKEN");
    CountDownLatch stop = new CountDownLatch(1);
    AtomicBoolean restart = new AtomicBoolean();
    ConfigPoller poller = null;
    if (adminBase != null && !adminBase.trim().isEmpty()) {
      if (machineToken == null || machineToken.trim().isEmpty())
        throw new IllegalStateException(
            "CANAL_ADMIN_MACHINE_TOKEN is required when Admin polling is enabled");
      URI uri =
          URI.create(
              adminBase.replaceAll("/$", "") + "/api/v1/runtime-config/" + selected.getNamespace());
      poller =
          new ConfigPoller(
              uri,
              machineToken,
              snapshots,
              content -> loader.parse(content),
              () -> {
                restart.set(true);
                stop.countDown();
              },
              Duration.ofSeconds(
                  Long.parseLong(env.getOrDefault("CANAL_ADMIN_POLL_SECONDS", "30"))));
      poller.start();
    }
    System.out.println("AI Canal READY");
    stop.await();
    if (poller != null) poller.close();
    runtime.close();
    if (restart.get()) System.exit(EXIT_CONFIG_RESTART);
  }
}
