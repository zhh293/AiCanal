package io.github.aicanal.spi;

public interface ResourceClassifier extends CanalPlugin {
  Classification classify(ParsedResource resource);
}
