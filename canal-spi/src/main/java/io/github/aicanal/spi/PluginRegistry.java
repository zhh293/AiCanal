package io.github.aicanal.spi;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public final class PluginRegistry {
  private final Map<Class<?>, Map<String, Supplier<?>>> factories = new ConcurrentHashMap<>();

  public <T extends CanalPlugin> void register(Class<T> contract, Supplier<? extends T> supplier) {
    T sample = supplier.get();
    String type = sample.type();
    sample.close();
    Map<String, Supplier<?>> types =
        factories.computeIfAbsent(contract, k -> new ConcurrentHashMap<>());
    if (types.putIfAbsent(type, supplier) != null)
      throw new IllegalStateException("duplicate SPI type " + type + " for " + contract.getName());
  }

  public <T extends CanalPlugin> void discover(Class<T> contract, ClassLoader loader) {
    for (T plugin : ServiceLoader.load(contract, loader)) register(contract, reflective(plugin));
  }

  private static <T extends CanalPlugin> Supplier<T> reflective(T p) {
    Class<? extends T> c = (Class<? extends T>) p.getClass();
    return () -> {
      try {
        return c.getDeclaredConstructor().newInstance();
      } catch (ReflectiveOperationException e) {
        throw new IllegalStateException("SPI needs public no-arg constructor: " + c.getName(), e);
      }
    };
  }

  public <T extends CanalPlugin> T create(Class<T> contract, String type) {
    Map<String, Supplier<?>> types = factories.getOrDefault(contract, Collections.emptyMap());
    Supplier<?> f = types.get(type);
    if (f == null)
      throw new IllegalArgumentException(
          "missing SPI " + contract.getName() + " type=" + type + "; available=" + types.keySet());
    return contract.cast(f.get());
  }

  public Set<String> types(Class<? extends CanalPlugin> contract) {
    return Collections.unmodifiableSet(
        factories.getOrDefault(contract, Collections.emptyMap()).keySet());
  }
}
