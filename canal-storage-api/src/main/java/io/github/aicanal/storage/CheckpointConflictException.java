package io.github.aicanal.storage;

public final class CheckpointConflictException extends RuntimeException {
  public CheckpointConflictException(String message) {
    super(message);
  }
}
