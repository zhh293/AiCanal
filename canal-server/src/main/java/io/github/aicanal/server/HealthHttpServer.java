package io.github.aicanal.server;

import com.sun.net.httpserver.*;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public final class HealthHttpServer implements AutoCloseable {
  private final HttpServer server;
  private final EmbeddedController controller;

  public HealthHttpServer(int port, EmbeddedController controller) throws IOException {
    this.controller = controller;
    server = HttpServer.create(new InetSocketAddress(port), 64);
    server.createContext(
        "/health/live", e -> reply(e, 200, "application/json", "{\"status\":\"UP\"}"));
    server.createContext(
        "/health/ready",
        e -> {
          Map<String, Object> s = controller.serverStatus();
          boolean ready = "READY".equals(s.get("status"));
          reply(e, ready ? 200 : 503, "application/json", json(s));
        });
    server.createContext(
        "/health/destinations",
        e -> reply(e, 200, "application/json", json(controller.listInstances())));
    server.createContext(
        "/metrics",
        e -> {
          Map<String, Object> s = controller.serverStatus();
          String body =
              "ai_canal_up 1\nai_canal_ready "
                  + ("READY".equals(s.get("status")) ? 1 : 0)
                  + "\nai_canal_instances "
                  + s.get("instances")
                  + "\nai_canal_uptime_milliseconds "
                  + s.get("uptimeMillis")
                  + "\n";
          reply(e, 200, "text/plain", body);
        });
  }

  public void start() {
    server.start();
  }

  public int port() {
    return server.getAddress().getPort();
  }

  public void close() {
    server.stop(1);
  }

  private static String json(Map<?, ?> m) {
    StringBuilder b = new StringBuilder("{");
    for (Map.Entry<?, ?> e : m.entrySet()) {
      if (b.length() > 1) b.append(',');
      b.append('"')
          .append(e.getKey())
          .append("\":\"")
          .append(String.valueOf(e.getValue()).replace("\"", "\\\""))
          .append('"');
    }
    return b.append('}').toString();
  }

  private static void reply(HttpExchange e, int status, String type, String body)
      throws IOException {
    byte[] b = body.getBytes(StandardCharsets.UTF_8);
    e.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
    e.sendResponseHeaders(status, b.length);
    try (OutputStream o = e.getResponseBody()) {
      o.write(b);
    }
  }
}
