package io.github.aicanal.admin.server;

import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.util.*;
import java.util.regex.Pattern;

public final class ConfigValidator {
  private static final Pattern NS = Pattern.compile("[a-z0-9-]+\\.[a-z0-9-]+\\.[a-z0-9-]+");
  private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory());

  public List<String> validate(String namespace, String content) {
    List<String> errors = new ArrayList<>();
    if (namespace == null || !NS.matcher(namespace).matches())
      errors.add("namespace must be environment.cluster.tenant");
    try {
      JsonNode root = yaml.readTree(content);
      if (root == null || !root.isObject())
        return Collections.singletonList("config root must be an object");
      if (containsSecret(root))
        errors.add("inline secret/password/token is forbidden; use a secret reference");
      JsonNode destinations = root.path("destinations");
      if (!destinations.isArray()) errors.add("destinations must be an array");
      else {
        Set<String> ids = new HashSet<>();
        for (JsonNode d : destinations) {
          String id = d.path("id").asText();
          if (id.isEmpty()) errors.add("destination id is required");
          else if (!ids.add(id)) errors.add("duplicate destination: " + id);
          String type = d.path("egress").path("type").asText();
          if (!Arrays.asList("TCP", "KAFKA", "ROCKETMQ", "RABBITMQ").contains(type))
            errors.add(id + ": exactly one valid egress.type is required");
          JsonNode ingress = d.path("ingress");
          if (ingress.path("maxBatchRecords").asInt(0) < 1)
            errors.add(id + ": maxBatchRecords must be positive");
          if (ingress.path("maxBatchBytes").asLong(0) < 1)
            errors.add(id + ": maxBatchBytes must be positive");
        }
      }
    } catch (Exception e) {
      errors.add("invalid YAML/JSON: " + e.getMessage());
    }
    return errors;
  }

  private boolean containsSecret(JsonNode node) {
    if (node.isObject()) {
      Iterator<Map.Entry<String, JsonNode>> it = node.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        String k = e.getKey().toLowerCase(Locale.ROOT);
        if ((k.contains("password") || k.equals("token") || k.equals("secret"))
            && e.getValue().isTextual()
            && !e.getValue().asText().startsWith("${secret:")) return true;
        if (containsSecret(e.getValue())) return true;
      }
    } else if (node.isArray()) for (JsonNode n : node) if (containsSecret(n)) return true;
    return false;
  }
}
