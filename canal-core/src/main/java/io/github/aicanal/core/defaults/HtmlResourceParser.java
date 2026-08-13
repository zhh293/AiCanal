package io.github.aicanal.core.defaults;

import io.github.aicanal.api.model.RawResource;
import io.github.aicanal.spi.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public final class HtmlResourceParser implements ResourceParser {
  public String type() {
    return "html-default";
  }

  public void initialize(PluginContext c, Map<String, Object> x) {}

  public ParsedResource parse(RawResource r) {
    String html = new String(r.getPayload(), StandardCharsets.UTF_8);
    String canonical =
        html.replaceAll("(?is)<script[^>]*>.*?</script>", "")
            .replaceAll("(?is)<style[^>]*>.*?</style>", "")
            .replaceAll("\\s+", " ")
            .trim();
    Map<String, String> a = new LinkedHashMap<>();
    java.util.regex.Matcher m =
        java.util.regex.Pattern.compile("(?is)<title[^>]*>(.*?)</title>").matcher(html);
    if (m.find()) a.put("title", m.group(1).replaceAll("<[^>]+>", "").trim());
    a.put("sourceUri", r.getSourceUri());
    return new ParsedResource(r, canonical.getBytes(StandardCharsets.UTF_8), a);
  }
}
