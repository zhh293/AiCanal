package io.github.aicanal.egress.rocketmq;

import io.github.aicanal.egress.MessageBatch;
import io.github.aicanal.egress.MessageQueueProducer;
import io.github.aicanal.egress.SendContext;
import io.github.aicanal.egress.SendResult;
import io.github.aicanal.spi.PluginContext;
import io.github.aicanal.storage.StoredEvent;
import java.time.Duration;
import java.util.*;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

public final class RocketMqProducerAdapter implements MessageQueueProducer {
  private DefaultMQProducer producer;
  private String topic;

  public String type() {
    return "rocketmq";
  }

  public void validate(Map<String, Object> c) {
    required(c, "nameServer");
    required(c, "topic");
  }

  public void initialize(PluginContext x, Map<String, Object> c) {
    validate(c);
    topic = String.valueOf(c.get("topic"));
    producer = new DefaultMQProducer("ai-canal-" + x.getDestination());
    producer.setNamesrvAddr(String.valueOf(c.get("nameServer")));
    producer.setSendMsgTimeout(
        Integer.parseInt(String.valueOf(c.getOrDefault("sendTimeoutMillis", 10000))));
  }

  public void start() {
    try {
      producer.start();
    } catch (Exception e) {
      throw new IllegalStateException("RocketMQ producer startup failed", e);
    }
  }

  public SendResult send(MessageBatch b, SendContext c) {
    try {
      for (StoredEvent s : b.getEvents()) {
        Message m =
            new Message(
                topic,
                s.getEvent().getCategory(),
                s.getEvent().getEventId(),
                s.getEvent().getPayload());
        m.putUserProperty("destination", c.getDestination());
        m.putUserProperty("offset", String.valueOf(s.getEvent().getOffset()));
        org.apache.rocketmq.client.producer.SendResult result = producer.send(m);
        if (result == null || result.getSendStatus() != SendStatus.SEND_OK)
          return SendResult.failed(true, "broker status not SEND_OK");
      }
      return SendResult.confirmed();
    } catch (Exception e) {
      return SendResult.failed(true, e.getClass().getSimpleName());
    }
  }

  public void flush(Duration t) {}

  public void close() {
    if (producer != null) producer.shutdown();
  }

  private static void required(Map<String, Object> c, String k) {
    if (!c.containsKey(k) || String.valueOf(c.get(k)).trim().isEmpty())
      throw new IllegalArgumentException("rocketmq requires " + k);
  }
}
