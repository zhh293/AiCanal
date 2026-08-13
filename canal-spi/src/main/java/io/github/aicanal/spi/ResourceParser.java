package io.github.aicanal.spi;

import io.github.aicanal.api.model.RawResource;

public interface ResourceParser extends CanalPlugin {
  ParsedResource parse(RawResource resource);
}
