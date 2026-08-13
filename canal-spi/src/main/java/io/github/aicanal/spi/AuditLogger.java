package io.github.aicanal.spi;

public interface AuditLogger extends CanalPlugin {
  void record(AuditEvent event);
}
