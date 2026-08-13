package io.github.aicanal.core.defaults;

import io.github.aicanal.spi.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class RuleResourceClassifier implements ResourceClassifier {
  private Map<String, List<String>> rules = Collections.emptyMap();

  public String type() {
    return "rule-default";
  }

  public void initialize(PluginContext c, Map<String, Object> x) {
    Map<String, List<String>> r = new LinkedHashMap<>();
    Object raw = x.get("rules");
    if (raw instanceof Map) {
      for (Map.Entry<?, ?> e : ((Map<?, ?>) raw).entrySet())
        r.put(String.valueOf(e.getKey()), values(e.getValue()));
    } else if (raw instanceof Iterable) {
      for (Object item : (Iterable<?>) raw)
        if (item instanceof Map) {
          Map<?, ?> m = (Map<?, ?>) item;
          Object category = m.get("category");
          if (category != null) r.put(String.valueOf(category), values(m.get("keywords")));
        }
    }
    rules = Collections.unmodifiableMap(r);
  }

  private static List<String> values(Object raw) {
    List<String> v = new ArrayList<>();
    if (raw instanceof Iterable) for (Object o : (Iterable<?>) raw) v.add(String.valueOf(o));
    return v;
  }

  public Classification classify(ParsedResource r) {
    String text =
        new String(r.getCanonicalContent(), StandardCharsets.UTF_8).toLowerCase(Locale.ROOT);
    for (Map.Entry<String, List<String>> e : rules.entrySet())
      for (String k : e.getValue())
        if (text.contains(k.toLowerCase(Locale.ROOT)))
          return new Classification(e.getKey(), Collections.emptyMap());
    return new Classification("uncategorized", Collections.emptyMap());
  }
}
