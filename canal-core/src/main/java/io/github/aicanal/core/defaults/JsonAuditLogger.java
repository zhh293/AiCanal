package io.github.aicanal.core.defaults;

import io.github.aicanal.spi.*;
import java.util.Map;
import java.util.logging.Logger;

public final class JsonAuditLogger implements AuditLogger {
  private static final Logger LOG = Logger.getLogger("ai-canal-audit");

  public String type() {
    return "slf4j-json";
  }

  public void initialize(PluginContext c, Map<String, Object> x) {}

  public void record(AuditEvent e) {
    LOG.info(
        "{\"timestamp\":\""
            + e.getTimestamp()
            + "\",\"destination\":\""
            + safe(e.getDestination())
            + "\",\"component\":\""
            + safe(e.getComponent())
            + "\",\"operation\":\""
            + safe(e.getOperation())
            + "\",\"result\":\""
            + safe(e.getResult())
            + "\",\"eventId\":\""
            + safe(e.getEventId())
            + "\",\"errorCode\":\""
            + safe(e.getErrorCode())
            + "\"}");
  }

  private static String safe(String s) {
    return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", " ");
  }
}
