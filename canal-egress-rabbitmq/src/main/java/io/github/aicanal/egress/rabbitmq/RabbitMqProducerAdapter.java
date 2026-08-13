package io.github.aicanal.egress.rabbitmq;

import com.rabbitmq.client.*;
import io.github.aicanal.egress.*;
import io.github.aicanal.spi.PluginContext;
import io.github.aicanal.storage.StoredEvent;
import java.net.URI;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RabbitMqProducerAdapter implements MessageQueueProducer {
  private Connection connection;
  private Channel channel;
  private String uri, exchange, routingKey;
  private final AtomicBoolean returned = new AtomicBoolean();

  public String type() {
    return "rabbitmq";
  }

  public void validate(Map<String, Object> c) {
    required(c, "uri");
    required(c, "exchange");
    required(c, "routingKey");
  }

  public void initialize(PluginContext x, Map<String, Object> c) {
    validate(c);
    uri = String.valueOf(c.get("uri"));
    exchange = String.valueOf(c.get("exchange"));
    routingKey = String.valueOf(c.get("routingKey"));
  }

  public void start() {
    try {
      ConnectionFactory f = new ConnectionFactory();
      f.setUri(new URI(uri));
      connection = f.newConnection("ai-canal-" + exchange);
      channel = connection.createChannel();
      channel.confirmSelect();
      channel.addReturnListener(
          (replyCode, replyText, ex, rk, properties, body) -> returned.set(true));
    } catch (Exception e) {
      throw new IllegalStateException("RabbitMQ producer startup failed", e);
    }
  }

  public synchronized SendResult send(MessageBatch b, SendContext c) {
    try {
      returned.set(false);
      for (StoredEvent s : b.getEvents()) {
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("eventId", s.getEvent().getEventId());
        headers.put("destination", c.getDestination());
        headers.put("offset", s.getEvent().getOffset());
        AMQP.BasicProperties props =
            new AMQP.BasicProperties.Builder()
                .deliveryMode(2)
                .contentType("application/octet-stream")
                .messageId(s.getEvent().getEventId())
                .headers(headers)
                .build();
        channel.basicPublish(exchange, routingKey, true, props, s.getEvent().getPayload());
      }
      boolean confirmed = channel.waitForConfirms(10000);
      return confirmed && !returned.get()
          ? SendResult.confirmed()
          : SendResult.failed(true, "publisher confirm missing or returned");
    } catch (Exception e) {
      return SendResult.failed(true, e.getClass().getSimpleName());
    }
  }

  public void flush(Duration t) {}

  public void close() {
    try {
      if (channel != null) channel.close();
    } catch (Exception ignored) {
    }
    try {
      if (connection != null) connection.close();
    } catch (Exception ignored) {
    }
  }

  private static void required(Map<String, Object> c, String k) {
    if (!c.containsKey(k) || String.valueOf(c.get(k)).trim().isEmpty())
      throw new IllegalArgumentException("rabbitmq requires " + k);
  }
}
