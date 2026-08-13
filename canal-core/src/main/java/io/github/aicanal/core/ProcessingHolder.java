package io.github.aicanal.core;

import io.github.aicanal.api.model.*;
import io.github.aicanal.spi.*;
import io.github.aicanal.storage.IngestRecord;

final class ProcessingHolder {
  IngestRecord ingest;
  int recordIndex;
  RawResource raw;
  ParsedResource parsed;
  Classification classification;
  CanalEvent event;
  Throwable error;

  void clear() {
    ingest = null;
    recordIndex = -1;
    raw = null;
    parsed = null;
    classification = null;
    event = null;
    error = null;
  }
}
