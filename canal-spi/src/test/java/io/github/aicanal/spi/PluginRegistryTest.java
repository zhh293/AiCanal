package io.github.aicanal.spi;

import static org.junit.jupiter.api.Assertions.*;

import io.github.aicanal.api.model.RawResource;
import java.util.*;
import org.junit.jupiter.api.Test;

class PluginRegistryTest {
  static final class P implements ResourceParser {
    public String type() {
      return "same";
    }

    public void initialize(PluginContext c, Map<String, Object> x) {}

    public ParsedResource parse(RawResource r) {
      return null;
    }
  }

  @Test
  void rejectsDuplicateTypeAndReportsMissing() {
    PluginRegistry r = new PluginRegistry();
    r.register(ResourceParser.class, P::new);
    assertThrows(IllegalStateException.class, () -> r.register(ResourceParser.class, P::new));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> r.create(ResourceParser.class, "missing"));
    assertTrue(e.getMessage().contains("available=[same]"));
  }
}
