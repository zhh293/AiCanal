package io.github.aicanal.storage.defaults;

enum WalRecordType {
  INGEST_RAW(1),
  EVENT_READY(2),
  PROCESS_REJECTED(3),
  DELIVERY_COMMIT(4),
  SEGMENT_SEAL(5);
  final int id;

  WalRecordType(int id) {
    this.id = id;
  }

  static WalRecordType from(int id) {
    for (WalRecordType t : values()) if (t.id == id) return t;
    throw new IllegalArgumentException("unknown WAL record type " + id);
  }
}
