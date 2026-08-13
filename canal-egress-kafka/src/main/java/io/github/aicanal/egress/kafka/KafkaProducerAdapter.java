package io.github.aicanal.egress.kafka;

import io.github.aicanal.egress.*;
import io.github.aicanal.spi.PluginContext;
import io.github.aicanal.storage.StoredEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class KafkaProducerAdapter implements MessageQueueProducer {
  private Producer<byte[], byte[]> producer;
  private String topic;

  public String type() {
    return "kafka";
  }

  public void validate(Map<String, Object> c) {
    required(c, "bootstrapServers");
    required(c, "topic");
  }

  public void initialize(PluginContext x, Map<String, Object> c) {
    validate(c);
    topic = String.valueOf(c.get("topic"));
    Properties p = new Properties();
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, c.get("bootstrapServers"));
    p.put(ProducerConfig.ACKS_CONFIG, String.valueOf(c.getOrDefault("acks", "all")));
    p.put(
        ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
        String.valueOf(c.getOrDefault("enableIdempotence", true)));
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    p.put(
        ProducerConfig.COMPRESSION_TYPE_CONFIG,
        String.valueOf(c.getOrDefault("compression", "none")));
    p.put(ProducerConfig.LINGER_MS_CONFIG, String.valueOf(c.getOrDefault("lingerMs", 5)));
    producer = new KafkaProducer<>(p);
  }

  public SendResult send(MessageBatch b, SendContext c) {
    try {
      for (StoredEvent s : b.getEvents()) {
        byte[] key = s.getEvent().getEventId().getBytes(StandardCharsets.UTF_8);
        ProducerRecord<byte[], byte[]> record =
            new ProducerRecord<>(
                topic,
                null,
                key,
                s.getEvent().getPayload(),
                Arrays.asList(
                    new RecordHeader("eventId", key),
                    new RecordHeader(
                        "destination", c.getDestination().getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader(
                        "offset",
                        String.valueOf(s.getEvent().getOffset())
                            .getBytes(StandardCharsets.UTF_8))));
        producer.send(record).get();
      }
      return SendResult.confirmed();
    } catch (Exception e) {
      return SendResult.failed(true, e.getClass().getSimpleName());
    }
  }

  public void flush(Duration timeout) {
    if (producer != null) producer.flush();
  }

  public void close() {
    if (producer != null) producer.close(Duration.ofSeconds(10));
  }

  private static void required(Map<String, Object> c, String k) {
    if (!c.containsKey(k) || String.valueOf(c.get(k)).trim().isEmpty())
      throw new IllegalArgumentException("kafka requires " + k);
  }
}
