package io.github.aicanal.ingress.netty;

import io.github.aicanal.api.error.CanalException;
import io.github.aicanal.api.model.*;
import io.github.aicanal.api.util.BatchChecksums;
import io.github.aicanal.spi.*;
import java.util.Map;

public final class NettyAgentDataReceiver implements AgentDataReceiver {
  private boolean requireChecksum = true;

  public String type() {
    return "netty-default";
  }

  public void initialize(PluginContext c, Map<String, Object> x) {
    requireChecksum =
        Boolean.parseBoolean(String.valueOf(x.getOrDefault("requireBatchChecksum", true)));
  }

  public ReceiveResult receive(AgentPublishRequest q, RawResourceSink sink) {
    if (requireChecksum && !BatchChecksums.sha256(q).equalsIgnoreCase(q.getBatchChecksum()))
      throw new CanalException(
          "BATCH_CHECKSUM_MISMATCH", "batch checksum does not match records", false);
    return new ReceiveResult(sink.append(q, Durability.GROUP_SYNC));
  }
}
