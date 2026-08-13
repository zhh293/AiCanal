package io.github.aicanal.admin.server;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.http.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;

class AdminHttpServerTest {
  private static final String TOKEN = "admin-test-token";
  private static final String YAML =
      String.join(
              "\n",
              "namespace: prod.main.tenant",
              "destinations:",
              "  - id: resources",
              "    ingress:",
              "      maxBatchRecords: 10",
              "      maxBatchBytes: 1024",
              "    egress:",
              "      type: TCP")
          + "\n";

  @Test
  void servesConsoleAndSupportsItsApiWorkflow() throws Exception {
    ConfigAdminService service = new ConfigAdminService(new ConfigValidator());
    Map<String, TokenAuthorizer.Role> tokens = new HashMap<>();
    tokens.put(TOKEN, TokenAuthorizer.Role.ADMIN);
    try (AdminHttpServer server =
        new AdminHttpServer(0, service, new TokenAuthorizer(tokens, "machine-token"))) {
      server.start();
      HttpClient client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
      String base = "http://127.0.0.1:" + server.port();

      HttpResponse<String> index = get(client, base + "/", false);
      assertEquals(200, index.statusCode());
      assertTrue(index.body().contains("AI CANAL"));
      assertTrue(index.headers().firstValue("Content-Security-Policy").isPresent());

      HttpResponse<String> script = get(client, base + "/assets/admin.js", false);
      assertEquals(200, script.statusCode());
      assertTrue(script.body().contains("ai-canal-token"));

      HttpResponse<String> session = get(client, base + "/api/v1/session", true);
      assertEquals(200, session.statusCode());
      assertTrue(session.body().contains("ADMIN"));

      HttpResponse<String> emptyNamespaces = get(client, base + "/api/v1/namespaces", true);
      assertEquals(200, emptyNamespaces.statusCode());
      assertEquals("[]", emptyNamespaces.body());

      String body =
          "{\"content\":\""
              + YAML.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
              + "\",\"comment\":\"console test\"}";
      HttpRequest releaseRequest =
          authorized(base + "/api/v1/namespaces/prod.main.tenant/releases")
              .header("Content-Type", "application/json")
              .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
              .build();
      HttpResponse<String> release =
          client.send(releaseRequest, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
      assertEquals(201, release.statusCode());
      assertTrue(release.body().contains("\"createdAt\":"));
      assertTrue(release.body().contains("prod.main.tenant"));

      HttpResponse<String> namespaces = get(client, base + "/api/v1/namespaces", true);
      assertEquals(200, namespaces.statusCode());
      assertEquals("[\"prod.main.tenant\"]", namespaces.body());

      HttpResponse<String> audit = get(client, base + "/api/v1/audit", true);
      assertEquals(200, audit.statusCode());
      assertTrue(audit.body().contains("operation=release"));
    }
  }

  @Test
  void protectsConsoleApisAndUnknownStaticPaths() throws Exception {
    ConfigAdminService service = new ConfigAdminService(new ConfigValidator());
    Map<String, TokenAuthorizer.Role> tokens = new HashMap<>();
    tokens.put(TOKEN, TokenAuthorizer.Role.VIEWER);
    try (AdminHttpServer server =
        new AdminHttpServer(0, service, new TokenAuthorizer(tokens, "machine-token"))) {
      server.start();
      HttpClient client = HttpClient.newHttpClient();
      String base = "http://127.0.0.1:" + server.port();
      assertEquals(401, get(client, base + "/api/v1/session", false).statusCode());
      assertEquals(404, get(client, base + "/not-a-console-route", false).statusCode());
    }
  }

  private static HttpResponse<String> get(HttpClient client, String url, boolean withToken)
      throws Exception {
    HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url)).GET();
    if (withToken) request.header("Authorization", "Bearer " + TOKEN);
    return client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
  }

  private static HttpRequest.Builder authorized(String url) {
    return HttpRequest.newBuilder(URI.create(url))
        .header("Authorization", "Bearer " + TOKEN)
        .header("X-Actor", "console-test");
  }
}
