package io.github.aicanal.admin.server;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.admin.api.*;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigAdminServiceTest {
  private static final String YAML =
      String.join(
              "\n",
              "destinations:",
              "  - id: d",
              "    ingress:",
              "      maxBatchRecords: 1",
              "      maxBatchBytes: 10",
              "    egress:",
              "      type: TCP")
          + "\n";
  @TempDir Path dir;

  @Test
  void releasesAreImmutablePublishedAndRollbackCreatesNewVersion() {
    ConfigAdminService s = new ConfigAdminService(new ConfigValidator());
    ConfigRelease one = s.release("prod.main.tenant", YAML, "editor", "first");
    assertEquals(1, one.getVersion());
    assertEquals(
        ReleaseStatus.PUBLISHED, s.publish(one.getNamespace(), 1, "publisher").getStatus());
    ConfigRelease rollback = s.rollback(one.getNamespace(), 1, "publisher", "rollback");
    assertEquals(2, rollback.getVersion());
    assertEquals(ReleaseStatus.PUBLISHED, rollback.getStatus());
    assertEquals(ReleaseStatus.SUPERSEDED, s.list(one.getNamespace()).get(0).getStatus());
    assertFalse(s.auditLog().isEmpty());
  }

  @Test
  void jdbcRepositorySurvivesServiceRecreation() {
    String url = "jdbc:h2:file:" + dir.resolve("admin").toString().replace('\\', '/');
    ConfigAdminService first =
        new ConfigAdminService(new ConfigValidator(), new JdbcConfigRepository(url, "sa", ""));
    first.publish(
        "prod.main.tenant", first.release("prod.main.tenant", YAML, "e", "c").getVersion(), "p");
    ConfigAdminService second =
        new ConfigAdminService(new ConfigValidator(), new JdbcConfigRepository(url, "sa", ""));
    assertEquals(1, second.published("prod.main.tenant").orElseThrow().getVersion());
  }

  @Test
  void rejectsInlineSecretAndBadNamespace() {
    ConfigAdminService s = new ConfigAdminService(new ConfigValidator());
    assertThrows(IllegalArgumentException.class, () -> s.release("bad", YAML, "x", ""));
    assertThrows(
        IllegalArgumentException.class,
        () -> s.release("prod.main.tenant", YAML + "password: plain\n", "x", ""));
  }

  @Test
  void validatesRaftMembershipBeforeRelease() {
    ConfigValidator validator = new ConfigValidator();
    String raft =
        String.join(
                "\n",
                "cluster:",
                "  mode: raft",
                "  raft:",
                "    bindAddress: 127.0.0.1:17001",
                "    peers: [n1@127.0.0.1:17001]",
                "server: {nodeId: n1}")
            + "\n"
            + YAML;
    assertTrue(validator.validate("prod.main.tenant", raft).isEmpty());
    assertTrue(
        validator.validate("prod.main.tenant", raft.replace("n1@", "other@")).stream()
            .anyMatch(error -> error.contains("server.nodeId")));
  }
}
