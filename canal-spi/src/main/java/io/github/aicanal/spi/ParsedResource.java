package io.github.aicanal.spi;

import io.github.aicanal.api.model.RawResource;
import java.util.*;

public final class ParsedResource {
  private final RawResource raw;
  private final byte[] canonicalContent;
  private final Map<String, String> attributes;

  public ParsedResource(RawResource raw, byte[] content, Map<String, String> attributes) {
    this.raw = raw;
    this.canonicalContent = Arrays.copyOf(content, content.length);
    this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
  }

  public RawResource getRaw() {
    return raw;
  }

  public byte[] getCanonicalContent() {
    return Arrays.copyOf(canonicalContent, canonicalContent.length);
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }
}
