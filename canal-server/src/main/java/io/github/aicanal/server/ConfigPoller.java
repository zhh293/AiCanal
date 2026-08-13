package io.github.aicanal.server;

import com.fasterxml.jackson.databind.*;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class ConfigPoller implements Runnable, AutoCloseable {
  private final HttpClient client =
      HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
  private final URI uri;
  private final String machineToken;
  private final ConfigSnapshotStore snapshots;
  private final Consumer<String> validator;
  private final Runnable restart;
  private final long intervalMillis;
  private final AtomicBoolean running = new AtomicBoolean();
  private volatile String etag = "";
  private Thread thread;
  private final ObjectMapper json = new ObjectMapper();

  public ConfigPoller(
      URI uri,
      String machineToken,
      ConfigSnapshotStore snapshots,
      Consumer<String> validator,
      Runnable restart,
      Duration interval) {
    this.uri = uri;
    this.machineToken = machineToken;
    this.snapshots = snapshots;
    this.validator = validator;
    this.restart = restart;
    this.intervalMillis = interval.toMillis();
  }

  public void start() {
    if (running.compareAndSet(false, true)) {
      thread = new Thread(this, "config-poller");
      thread.setDaemon(true);
      thread.start();
    }
  }

  public void run() {
    while (running.get()) {
      try {
        HttpRequest.Builder b =
            HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(5))
                .header("Authorization", "Bearer " + machineToken)
                .GET();
        if (!etag.isEmpty()) b.header("If-None-Match", etag);
        HttpResponse<String> r = client.send(b.build(), HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() == 200) {
          JsonNode n = json.readTree(r.body());
          String content =
              n.path("config").isTextual()
                  ? n.path("config").asText()
                  : n.path("config").toString();
          String hash = n.path("contentHash").asText();
          long version = n.path("version").asLong();
          validator.accept(content);
          snapshots.savePending(content, hash, version);
          etag = '"' + hash + '"';
          restart.run();
          return;
        }
      } catch (Exception ignored) {
      }
      try {
        Thread.sleep(intervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  public void close() {
    running.set(false);
    if (thread != null) thread.interrupt();
  }
}
