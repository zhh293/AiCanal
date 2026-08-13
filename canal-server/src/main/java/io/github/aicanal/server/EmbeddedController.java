package io.github.aicanal.server;

import io.github.aicanal.api.model.*;
import io.github.aicanal.egress.netty.TcpSubscription;
import io.github.aicanal.storage.StoredEvent;
import java.util.*;

public interface EmbeddedController {
  Map<String, Object> serverStatus();

  InstanceState instanceStatus(String destination);

  Map<String, InstanceState> listInstances();

  IngressAck publish(AgentPublishRequest request);

  void pause(String destination);

  void resume(String destination);

  List<StoredEvent> inspectEvents(String destination, long afterOffset, int limit);

  Map<String, Object> deliveryStatus(String destination);

  TcpSubscription subscribe(String destination, String consumerId);
}
