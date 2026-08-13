package io.github.aicanal.admin.server;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public final class CanalAdminMain {
  private CanalAdminMain() {}

  public static void main(String[] args) throws Exception {
    Map<String, String> env = System.getenv();
    int port = Integer.parseInt(env.getOrDefault("CANAL_ADMIN_PORT", "8080"));
    String jdbc = env.getOrDefault("CANAL_ADMIN_JDBC_URL", "jdbc:h2:file:./data/admin/ai-canal");
    String user = env.getOrDefault("CANAL_ADMIN_DB_USER", "sa"),
        password = env.getOrDefault("CANAL_ADMIN_DB_PASSWORD", "");
    Map<String, TokenAuthorizer.Role> tokens = new LinkedHashMap<>();
    put(tokens, env, "CANAL_ADMIN_VIEWER_TOKEN", TokenAuthorizer.Role.VIEWER);
    put(tokens, env, "CANAL_ADMIN_EDITOR_TOKEN", TokenAuthorizer.Role.EDITOR);
    put(tokens, env, "CANAL_ADMIN_PUBLISHER_TOKEN", TokenAuthorizer.Role.PUBLISHER);
    put(tokens, env, "CANAL_ADMIN_ADMIN_TOKEN", TokenAuthorizer.Role.ADMIN);
    if (tokens.isEmpty())
      throw new IllegalStateException(
          "at least one Admin bearer token must be provided through environment");
    String machine = required(env, "CANAL_ADMIN_MACHINE_TOKEN");
    ConfigAdminService service =
        new ConfigAdminService(
            new ConfigValidator(), new JdbcConfigRepository(jdbc, user, password));
    AdminHttpServer server =
        new AdminHttpServer(port, service, new TokenAuthorizer(tokens, machine));
    Runtime.getRuntime().addShutdownHook(new Thread(server::close, "admin-shutdown"));
    server.start();
    System.out.println("AI Canal Admin READY port=" + port);
    new CountDownLatch(1).await();
  }

  private static void put(
      Map<String, TokenAuthorizer.Role> tokens,
      Map<String, String> env,
      String key,
      TokenAuthorizer.Role role) {
    String token = env.get(key);
    if (token != null && !token.trim().isEmpty()) tokens.put(token, role);
  }

  private static String required(Map<String, String> env, String key) {
    String v = env.get(key);
    if (v == null || v.trim().isEmpty()) throw new IllegalStateException(key + " is required");
    return v;
  }
}
