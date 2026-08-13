package io.github.aicanal.egress;

public interface EgressRuntime extends AutoCloseable {
  void start();

  String state();

  @Override
  void close();
}
