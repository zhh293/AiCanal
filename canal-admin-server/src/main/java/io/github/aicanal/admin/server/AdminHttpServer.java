package io.github.aicanal.admin.server;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.sun.net.httpserver.*;
import io.github.aicanal.admin.api.ConfigRelease;
import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.*;

public final class AdminHttpServer implements AutoCloseable {
  private final HttpServer server;
  private final ConfigAdminService service;
  private final TokenAuthorizer auth;
  private final ObjectMapper json = jsonMapper();

  public AdminHttpServer(int port, ConfigAdminService service, TokenAuthorizer auth)
      throws IOException {
    this.service = service;
    this.auth = auth;
    server = HttpServer.create(new InetSocketAddress(port), 128);
    server.createContext("/api/v1/session", this::session);
    server.createContext("/api/v1/audit", this::audit);
    server.createContext("/api/v1/runtime-config", this::runtime);
    server.createContext("/api/v1/namespaces", this::manage);
    server.createContext(
        "/health/live", e -> reply(e, 200, "application/json", "{\"status\":\"UP\"}"));
    server.createContext("/", this::staticResource);
    server.setExecutor(
        java.util.concurrent.Executors.newFixedThreadPool(
            Math.max(4, Runtime.getRuntime().availableProcessors())));
  }

  private static ObjectMapper jsonMapper() {
    ObjectMapper mapper = new ObjectMapper();
    SimpleModule time = new SimpleModule();
    time.addSerializer(
        Instant.class,
        new JsonSerializer<Instant>() {
          @Override
          public void serialize(
              Instant value,
              com.fasterxml.jackson.core.JsonGenerator generator,
              SerializerProvider p)
              throws IOException {
            generator.writeString(value.toString());
          }
        });
    return mapper.registerModule(time);
  }

  private void session(HttpExchange e) throws IOException {
    try {
      if (!"GET".equals(e.getRequestMethod())) {
        reply(e, 405, "application/json", error("method not allowed"));
        return;
      }
      TokenAuthorizer.Role role =
          auth.require(
              e,
              TokenAuthorizer.Role.VIEWER,
              TokenAuthorizer.Role.EDITOR,
              TokenAuthorizer.Role.PUBLISHER);
      Map<String, String> response = new LinkedHashMap<>();
      response.put("role", role.name());
      response.put("actor", actor(e, role));
      reply(e, 200, "application/json", json.writeValueAsString(response));
    } catch (SecurityException x) {
      reply(e, 401, "application/json", error(x.getMessage()));
    }
  }

  private void audit(HttpExchange e) throws IOException {
    try {
      if (!"GET".equals(e.getRequestMethod())) {
        reply(e, 405, "application/json", error("method not allowed"));
        return;
      }
      auth.require(
          e,
          TokenAuthorizer.Role.VIEWER,
          TokenAuthorizer.Role.EDITOR,
          TokenAuthorizer.Role.PUBLISHER);
      reply(e, 200, "application/json", json.writeValueAsString(service.auditLog()));
    } catch (SecurityException x) {
      reply(e, 403, "application/json", error(x.getMessage()));
    } catch (Exception x) {
      reply(e, 400, "application/json", error(x.getMessage()));
    }
  }

  private void runtime(HttpExchange e) throws IOException {
    try {
      auth.requireMachine(e);
      if (!"GET".equals(e.getRequestMethod())) {
        reply(e, 405, "text/plain", "");
        return;
      }
      String[] p = parts(e);
      if (p.length < 5) {
        reply(e, 400, "text/plain", "missing namespace");
        return;
      }
      Optional<ConfigRelease> r = service.published(p[4]);
      if (!r.isPresent()) {
        reply(e, 404, "text/plain", "not found");
        return;
      }
      String etag = '"' + r.get().getContentHash() + '"';
      if (etag.equals(e.getRequestHeaders().getFirst("If-None-Match"))) {
        e.getResponseHeaders().set("ETag", etag);
        e.sendResponseHeaders(304, -1);
        e.close();
        return;
      }
      e.getResponseHeaders().set("ETag", etag);
      reply(
          e,
          200,
          "application/json",
          json.writeValueAsString(new io.github.aicanal.admin.api.RuntimeConfigResponse(r.get())));
    } catch (SecurityException x) {
      reply(e, 401, "application/json", error(x.getMessage()));
    } catch (Exception x) {
      reply(e, 400, "application/json", error(x.getMessage()));
    }
  }

  private void manage(HttpExchange e) throws IOException {
    try {
      String[] p = parts(e);
      String method = e.getRequestMethod();
      if (p.length == 4 && "GET".equals(method)) {
        auth.require(
            e,
            TokenAuthorizer.Role.VIEWER,
            TokenAuthorizer.Role.EDITOR,
            TokenAuthorizer.Role.PUBLISHER);
        reply(e, 200, "application/json", json.writeValueAsString(service.namespaces()));
        return;
      }
      if (p.length < 5) throw new IllegalArgumentException("namespace required");
      String ns = p[4];
      if (p.length == 6 && "releases".equals(p[5]) && "GET".equals(method)) {
        auth.require(
            e,
            TokenAuthorizer.Role.VIEWER,
            TokenAuthorizer.Role.EDITOR,
            TokenAuthorizer.Role.PUBLISHER);
        reply(e, 200, "application/json", json.writeValueAsString(service.list(ns)));
        return;
      }
      if (p.length == 6 && "validate".equals(p[5]) && "POST".equals(method)) {
        auth.require(e, TokenAuthorizer.Role.EDITOR);
        JsonNode body = body(e);
        reply(
            e,
            200,
            "application/json",
            json.writeValueAsString(service.validate(ns, body.path("content").asText())));
        return;
      }
      if (p.length == 6 && "releases".equals(p[5]) && "POST".equals(method)) {
        TokenAuthorizer.Role role = auth.require(e, TokenAuthorizer.Role.EDITOR);
        JsonNode b = body(e);
        reply(
            e,
            201,
            "application/json",
            json.writeValueAsString(
                service.release(
                    ns, b.path("content").asText(), actor(e, role), b.path("comment").asText())));
        return;
      }
      if (p.length >= 8 && "releases".equals(p[5])) {
        long version = Long.parseLong(p[6]);
        if ("publish".equals(p[7]) && "POST".equals(method)) {
          TokenAuthorizer.Role role = auth.require(e, TokenAuthorizer.Role.PUBLISHER);
          reply(
              e,
              200,
              "application/json",
              json.writeValueAsString(service.publish(ns, version, actor(e, role))));
          return;
        }
        if ("rollback".equals(p[7]) && "POST".equals(method)) {
          TokenAuthorizer.Role role = auth.require(e, TokenAuthorizer.Role.PUBLISHER);
          JsonNode b = body(e);
          reply(
              e,
              201,
              "application/json",
              json.writeValueAsString(
                  service.rollback(ns, version, actor(e, role), b.path("comment").asText())));
          return;
        }
        if ("diff".equals(p[7]) && p.length >= 9 && "GET".equals(method)) {
          auth.require(
              e,
              TokenAuthorizer.Role.VIEWER,
              TokenAuthorizer.Role.EDITOR,
              TokenAuthorizer.Role.PUBLISHER);
          reply(e, 200, "text/plain", service.diff(ns, version, Long.parseLong(p[8])));
          return;
        }
      }
      reply(e, 404, "application/json", error("route not found"));
    } catch (SecurityException x) {
      reply(e, 403, "application/json", error(x.getMessage()));
    } catch (Exception x) {
      reply(e, 400, "application/json", error(x.getMessage()));
    }
  }

  private JsonNode body(HttpExchange e) throws IOException {
    return json.readTree(e.getRequestBody());
  }

  private static String actor(HttpExchange e, TokenAuthorizer.Role role) {
    String a = e.getRequestHeaders().getFirst("X-Actor");
    return a == null || a.trim().isEmpty() ? role.name().toLowerCase(Locale.ROOT) : a;
  }

  private static String[] parts(HttpExchange e) {
    return e.getRequestURI().getPath().split("/");
  }

  private void staticResource(HttpExchange e) throws IOException {
    String method = e.getRequestMethod();
    if (!"GET".equals(method) && !"HEAD".equals(method)) {
      reply(e, 405, "text/plain", "method not allowed");
      return;
    }
    String path = e.getRequestURI().getPath();
    String resource;
    String type;
    if ("/".equals(path) || "/index.html".equals(path)) {
      resource = "/admin-ui/index.html";
      type = "text/html";
    } else if ("/assets/admin.css".equals(path)) {
      resource = "/admin-ui/admin.css";
      type = "text/css";
    } else if ("/assets/admin.js".equals(path)) {
      resource = "/admin-ui/admin.js";
      type = "application/javascript";
    } else if ("/favicon.svg".equals(path)) {
      resource = "/admin-ui/favicon.svg";
      type = "image/svg+xml";
    } else {
      reply(e, 404, "text/plain", "not found");
      return;
    }
    try (InputStream input = AdminHttpServer.class.getResourceAsStream(resource)) {
      if (input == null) {
        reply(e, 404, "text/plain", "not found");
        return;
      }
      byte[] body = input.readAllBytes();
      securityHeaders(e);
      e.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
      e.getResponseHeaders().set("Cache-Control", "no-cache");
      if ("HEAD".equals(method)) {
        e.sendResponseHeaders(200, -1);
        e.close();
        return;
      }
      e.sendResponseHeaders(200, body.length);
      try (OutputStream output = e.getResponseBody()) {
        output.write(body);
      }
    }
  }

  private static void securityHeaders(HttpExchange e) {
    e.getResponseHeaders()
        .set(
            "Content-Security-Policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; "
                + "connect-src 'self'; font-src 'self'; object-src 'none'; base-uri 'none'; "
                + "frame-ancestors 'none'; form-action 'self'");
    e.getResponseHeaders().set("Referrer-Policy", "no-referrer");
    e.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
    e.getResponseHeaders().set("X-Frame-Options", "DENY");
  }

  private static String error(String m) {
    return "{\"error\":\""
        + (m == null ? "unknown" : m.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " "))
        + "\"}";
  }

  private static void reply(HttpExchange e, int status, String type, String text)
      throws IOException {
    byte[] b = text.getBytes(StandardCharsets.UTF_8);
    securityHeaders(e);
    e.getResponseHeaders().set("Cache-Control", "no-store");
    e.getResponseHeaders().set("Content-Type", type + "; charset=utf-8");
    e.sendResponseHeaders(status, b.length);
    try (OutputStream o = e.getResponseBody()) {
      o.write(b);
    }
  }

  public void start() {
    server.start();
  }

  int port() {
    return server.getAddress().getPort();
  }

  public void close() {
    server.stop(1);
  }
}
