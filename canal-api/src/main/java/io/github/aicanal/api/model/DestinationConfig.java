package io.github.aicanal.api.model;

import java.util.Objects;
import java.util.regex.Pattern;

public final class DestinationConfig {
  private static final Pattern ID = Pattern.compile("[a-z0-9][a-z0-9._-]{0,126}");
  private final String id;
  private final boolean enabled;
  private final PluginConfig receiver, parser, classifier, deduplicator, logger, storage;
  private final IngressPolicy ingress;
  private final EgressConfig egress;

  public DestinationConfig(
      String id,
      boolean enabled,
      PluginConfig receiver,
      IngressPolicy ingress,
      PluginConfig parser,
      PluginConfig classifier,
      PluginConfig deduplicator,
      PluginConfig logger,
      PluginConfig storage,
      EgressConfig egress) {
    if (id == null || !ID.matcher(id).matches())
      throw new IllegalArgumentException("invalid destination id: " + id);
    this.id = id;
    this.enabled = enabled;
    this.receiver = Objects.requireNonNull(receiver);
    this.ingress = Objects.requireNonNull(ingress);
    this.parser = Objects.requireNonNull(parser);
    this.classifier = Objects.requireNonNull(classifier);
    this.deduplicator = Objects.requireNonNull(deduplicator);
    this.logger = Objects.requireNonNull(logger);
    this.storage = Objects.requireNonNull(storage);
    this.egress = Objects.requireNonNull(egress);
  }

  public String getId() {
    return id;
  }

  public boolean isEnabled() {
    return enabled;
  }

  public PluginConfig getReceiver() {
    return receiver;
  }

  public IngressPolicy getIngress() {
    return ingress;
  }

  public PluginConfig getParser() {
    return parser;
  }

  public PluginConfig getClassifier() {
    return classifier;
  }

  public PluginConfig getDeduplicator() {
    return deduplicator;
  }

  public PluginConfig getLogger() {
    return logger;
  }

  public PluginConfig getStorage() {
    return storage;
  }

  public EgressConfig getEgress() {
    return egress;
  }
}
