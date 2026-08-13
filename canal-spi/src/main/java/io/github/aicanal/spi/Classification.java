package io.github.aicanal.spi;

import java.util.*;

public final class Classification {
  private final String category;
  private final Map<String, String> attributes;

  public Classification(String category, Map<String, String> attributes) {
    this.category = category;
    this.attributes = Collections.unmodifiableMap(new LinkedHashMap<>(attributes));
  }

  public String getCategory() {
    return category;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }
}
