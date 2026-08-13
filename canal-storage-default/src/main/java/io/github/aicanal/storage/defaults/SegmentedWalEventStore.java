package io.github.aicanal.storage.defaults;

import static java.nio.file.StandardOpenOption.*;

import io.github.aicanal.api.error.CanalException;
import io.github.aicanal.api.model.*;
import io.github.aicanal.storage.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.CRC32C;

public final class SegmentedWalEventStore implements EventStore {
  private static final int MAGIC = 0x41494357,
      HEADER = 20,
      TRAILER = 4,
      MAX_RECORD = 64 * 1024 * 1024;
  private static final byte VERSION = 1;
  private final String destination, configVersion, nodeId;
  private final Path walDir;
  private final long segmentMaxBytes, groupSyncIntervalNanos;
  private final int groupSyncMaxRecords;
  private final ReentrantLock lock = new ReentrantLock();
  private final Condition groupRequested = lock.newCondition(),
      groupCompleted = lock.newCondition();
  private final Map<String, IngressAck> requestIndex = new HashMap<>();
  private final Map<String, String> requestChecksums = new HashMap<>();
  private final Map<Long, IngestRecord> ingests = new TreeMap<>();
  private final Set<String> processedRecords = new HashSet<>();
  private final NavigableMap<Long, StoredEvent> ready = new TreeMap<>();
  private final Map<String, StoredEvent> events = new HashMap<>();
  private final Map<String, DeliveryCheckpoint> checkpoints = new HashMap<>();
  private long sequence, readyOffset, syncedSequence;
  private int pendingGroupRecords;
  private boolean groupRunning;
  private Thread groupThread;
  private FileChannel channel, indexChannel;
  private Path activePath, indexPath;

  public SegmentedWalEventStore(
      Path dataDir, String destination, long segmentMaxBytes, String configVersion, String nodeId) {
    this(dataDir, destination, segmentMaxBytes, configVersion, nodeId, 5, 256);
  }

  public SegmentedWalEventStore(
      Path dataDir,
      String destination,
      long segmentMaxBytes,
      String configVersion,
      String nodeId,
      long groupSyncIntervalMillis,
      int groupSyncMaxRecords) {
    this.destination = destination;
    this.walDir = dataDir.resolve(destination).resolve("wal");
    this.segmentMaxBytes = segmentMaxBytes;
    this.configVersion = configVersion;
    this.nodeId = nodeId;
    if (groupSyncIntervalMillis < 1 || groupSyncMaxRecords < 1)
      throw new IllegalArgumentException("invalid group sync settings");
    this.groupSyncIntervalNanos = TimeUnit.MILLISECONDS.toNanos(groupSyncIntervalMillis);
    this.groupSyncMaxRecords = groupSyncMaxRecords;
    open();
    startGroupCommitter();
  }

  private void open() {
    lock.lock();
    try {
      Files.createDirectories(walDir);
      scan();
      openActive();
      syncedSequence = sequence;
    } catch (IOException e) {
      throw new CanalException("WAL_OPEN_FAILED", e.getMessage(), true, e);
    } finally {
      lock.unlock();
    }
  }

  private void startGroupCommitter() {
    groupRunning = true;
    groupThread = new Thread(this::groupCommitLoop, "wal-group-sync-" + destination);
    groupThread.setDaemon(true);
    groupThread.start();
  }

  private void openActive() throws IOException {
    activePath = walDir.resolve(String.format("%020d.log", sequence + 1));
    indexPath = indexPath(activePath);
    channel = FileChannel.open(activePath, CREATE, READ, WRITE);
    channel.position(channel.size());
    indexChannel = FileChannel.open(indexPath, CREATE, WRITE);
    indexChannel.position(indexChannel.size());
  }

  @Override
  public IngressAck appendIngress(AgentPublishRequest q, Durability d) {
    lock.lock();
    try {
      IngressAck old = requestIndex.get(q.idempotencyKey());
      if (old != null) {
        if (!Objects.equals(requestChecksums.get(q.idempotencyKey()), q.getBatchChecksum()))
          throw new CanalException(
              "REQUEST_ID_CONFLICT", "same requestId has different checksum", false);
        return new IngressAck(old.getRequestId(), old.getIngestSequence(), true);
      }
      long seq = append(WalRecordType.INGEST_RAW, WalCodec.ingress(q), d);
      IngressAck ack = new IngressAck(q.getRequestId(), seq, false);
      requestIndex.put(q.idempotencyKey(), ack);
      requestChecksums.put(q.idempotencyKey(), q.getBatchChecksum());
      ingests.put(seq, new IngestRecord(seq, q));
      return ack;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public StoredEvent appendReady(long ingestSequence, int recordIndex, CanalEvent event) {
    lock.lock();
    try {
      StoredEvent existing = events.get(event.getEventId());
      if (existing != null) {
        appendRejected(
            ingestSequence, recordIndex, "DUPLICATE_EVENT", "existing " + event.getEventId());
        return existing;
      }
      long eventWalSequence = sequence + 1;
      CanalEvent numbered = event.withOffset(eventWalSequence);
      long appended =
          append(
              WalRecordType.EVENT_READY,
              WalCodec.ready(ingestSequence, recordIndex, numbered, configVersion, nodeId),
              Durability.GROUP_SYNC);
      if (appended != eventWalSequence)
        throw new IllegalStateException("EVENT_READY sequence changed while locked");
      StoredEvent s =
          new StoredEvent(
              numbered, StoredEvent.State.READY, Instant.now(), null, 0, "", configVersion, nodeId);
      ready.put(appended, s);
      events.put(numbered.getEventId(), s);
      markProcessed(ingestSequence, recordIndex);
      readyOffset = appended;
      return s;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void appendRejected(
      long ingestSequence, int recordIndex, String errorCode, String summary) {
    lock.lock();
    try {
      if (isProcessed(ingestSequence, recordIndex)) return;
      append(
          WalRecordType.PROCESS_REJECTED,
          WalCodec.rejected(ingestSequence, recordIndex, errorCode, summary),
          Durability.GROUP_SYNC);
      markProcessed(ingestSequence, recordIndex);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public List<StoredEvent> readAfter(long offset, int limit, int maxBytes) {
    if (limit < 1 || maxBytes < 1) throw new IllegalArgumentException();
    lock.lock();
    try {
      List<StoredEvent> out = new ArrayList<>();
      int bytes = 0;
      for (StoredEvent e : ready.tailMap(offset, false).values()) {
        int n = e.getEvent().getPayload().length;
        if (!out.isEmpty() && bytes + n > maxBytes) break;
        out.add(e);
        bytes += n;
        if (out.size() >= limit) break;
      }
      return out;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<StoredEvent> findByEventId(String id) {
    lock.lock();
    try {
      return Optional.ofNullable(events.get(id));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<IngestRecord> findIngress(long seq) {
    lock.lock();
    try {
      return Optional.ofNullable(ingests.get(seq));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public boolean isProcessed(long ingestSequence, int recordIndex) {
    lock.lock();
    try {
      return processedRecords.contains(processedKey(ingestSequence, recordIndex));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public RecoveryPlan recover() {
    lock.lock();
    try {
      List<IngestRecord> p = new ArrayList<>();
      for (IngestRecord r : ingests.values()) {
        boolean pending = false;
        for (int i = 0; i < r.getRequest().getRecords().size(); i++)
          if (!isProcessed(r.getSequence(), i)) {
            pending = true;
            break;
          }
        if (pending) p.add(r);
      }
      return new RecoveryPlan(p, sequence, readyOffset);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public DeliveryCheckpoint checkpoint(String id, long epoch) {
    lock.lock();
    try {
      return checkpoints.getOrDefault(id, DeliveryCheckpoint.initial(destination, id, epoch));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public DeliveryCheckpoint commitDelivery(
      String id, long offset, long expectedVersion, long epoch) {
    lock.lock();
    try {
      DeliveryCheckpoint cur = checkpoint(id, epoch);
      if (cur.getVersion() != expectedVersion)
        throw new CheckpointConflictException("checkpoint version changed");
      if (offset < cur.getCommittedOffset())
        throw new CheckpointConflictException("checkpoint cannot move backwards");
      if (cur.getVersion() > 0 && cur.getLeaderEpoch() != epoch)
        throw new CheckpointConflictException("leader epoch changed");
      if (offset > readyOffset)
        throw new CheckpointConflictException("offset beyond ready watermark");
      DeliveryCheckpoint next = cur.advance(offset, epoch);
      append(WalRecordType.DELIVERY_COMMIT, WalCodec.checkpoint(next), Durability.SYNC);
      checkpoints.put(id, next);
      return next;
    } finally {
      lock.unlock();
    }
  }

  private long append(WalRecordType type, byte[] payload, Durability durability) {
    try {
      if (payload.length > MAX_RECORD)
        throw new CanalException("WAL_RECORD_TOO_LARGE", "record too large", false);
      if (channel.size() + HEADER + payload.length + TRAILER > segmentMaxBytes
          && channel.size() > 0) roll();
      long position = channel.position(), seq = ++sequence;
      ByteBuffer body = ByteBuffer.allocate(HEADER + payload.length).order(ByteOrder.BIG_ENDIAN);
      body.putInt(MAGIC)
          .put(VERSION)
          .put((byte) type.id)
          .putShort((short) 0)
          .putInt(payload.length)
          .putLong(seq)
          .put(payload)
          .flip();
      CRC32C crc = new CRC32C();
      crc.update(body.asReadOnlyBuffer());
      ByteBuffer frame = ByteBuffer.allocate(body.remaining() + TRAILER);
      frame.put(body).putInt((int) crc.getValue()).flip();
      while (frame.hasRemaining()) channel.write(frame);
      if (shouldIndex(seq, type))
        appendIndex(
            indexChannel, seq, position, type == WalRecordType.EVENT_READY ? seq : readyOffset);
      if (durability == Durability.SYNC) {
        channel.force(false);
        syncedSequence = Math.max(syncedSequence, seq);
        groupCompleted.signalAll();
      } else if (durability == Durability.GROUP_SYNC) {
        pendingGroupRecords++;
        groupRequested.signal();
        while (groupRunning && syncedSequence < seq) groupCompleted.await();
        if (syncedSequence < seq)
          throw new CanalException(
              "WAL_GROUP_SYNC_STOPPED", "group committer stopped before fsync", true);
      }
      return seq;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new CanalException(
          "WAL_GROUP_SYNC_INTERRUPTED", "interrupted waiting for fsync", true, e);
    } catch (IOException e) {
      throw new CanalException("WAL_APPEND_FAILED", e.getMessage(), true, e);
    }
  }

  private void groupCommitLoop() {
    lock.lock();
    try {
      while (groupRunning) {
        while (groupRunning && pendingGroupRecords == 0) groupRequested.await();
        if (!groupRunning) break;
        long deadline = System.nanoTime() + groupSyncIntervalNanos;
        while (groupRunning && pendingGroupRecords < groupSyncMaxRecords) {
          long remaining = deadline - System.nanoTime();
          if (remaining <= 0) break;
          groupRequested.awaitNanos(remaining);
        }
        if (pendingGroupRecords > 0) {
          try {
            channel.force(false);
            syncedSequence = sequence;
            pendingGroupRecords = 0;
            groupCompleted.signalAll();
          } catch (IOException e) {
            groupRunning = false;
            groupCompleted.signalAll();
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      groupRunning = false;
      groupCompleted.signalAll();
      lock.unlock();
    }
  }

  private void roll() throws IOException {
    channel.force(true);
    indexChannel.force(true);
    channel.close();
    indexChannel.close();
    openActive();
  }

  private void scan() throws IOException {
    List<Path> paths = new ArrayList<>();
    try (DirectoryStream<Path> s = Files.newDirectoryStream(walDir, "*.log")) {
      for (Path p : s) paths.add(p);
    }
    paths.sort(Comparator.comparing(Path::getFileName));
    for (int i = 0; i < paths.size(); i++) scanFile(paths.get(i), i == paths.size() - 1);
  }

  private void scanFile(Path p, boolean active) throws IOException {
    Path rebuilt = Files.createTempFile(walDir, p.getFileName().toString(), ".idx.tmp");
    boolean valid = false;
    try (FileChannel c = FileChannel.open(p, READ, active ? WRITE : READ);
        FileChannel idx = FileChannel.open(rebuilt, WRITE, TRUNCATE_EXISTING)) {
      long position = 0, lastGood = 0, size = c.size();
      while (position < size) {
        if (size - position < HEADER + TRAILER) {
          if (active) {
            c.truncate(lastGood);
            break;
          }
          throw new IOException("truncated sealed segment " + p);
        }
        ByteBuffer h = ByteBuffer.allocate(HEADER).order(ByteOrder.BIG_ENDIAN);
        readFully(c, h, position);
        h.flip();
        int magic = h.getInt();
        byte version = h.get();
        int typeId = h.get() & 255;
        h.getShort();
        int length = h.getInt();
        long seq = h.getLong();
        if (magic != MAGIC || version != VERSION || length < 0 || length > MAX_RECORD)
          throw new IOException("invalid WAL header at " + p + ":" + position);
        long frame = HEADER + (long) length + TRAILER;
        if (position + frame > size) {
          if (active) {
            c.truncate(lastGood);
            break;
          }
          throw new IOException("truncated sealed segment " + p);
        }
        ByteBuffer all = ByteBuffer.allocate(HEADER + length);
        readFully(c, all, position);
        all.flip();
        CRC32C crc = new CRC32C();
        crc.update(all.asReadOnlyBuffer());
        ByteBuffer tail = ByteBuffer.allocate(4);
        readFully(c, tail, position + HEADER + length);
        tail.flip();
        if ((int) crc.getValue() != tail.getInt())
          throw new IOException("CRC mismatch at " + p + ":" + position);
        all.position(HEADER);
        byte[] payload = new byte[length];
        all.get(payload);
        WalRecordType type = WalRecordType.from(typeId);
        apply(type, seq, payload);
        sequence = Math.max(sequence, seq);
        if (shouldIndex(seq, type)) appendIndex(idx, seq, position, readyOffset);
        lastGood = position + frame;
        position = lastGood;
      }
      idx.force(true);
      valid = true;
    } finally {
      if (valid) moveAtomic(rebuilt, indexPath(p));
      else Files.deleteIfExists(rebuilt);
    }
  }

  private static boolean shouldIndex(long seq, WalRecordType type) {
    return seq % 64 == 1
        || type == WalRecordType.EVENT_READY
        || type == WalRecordType.DELIVERY_COMMIT;
  }

  private static void appendIndex(
      FileChannel idx, long sequence, long filePosition, long readyOffset) throws IOException {
    ByteBuffer b = ByteBuffer.allocate(24).order(ByteOrder.BIG_ENDIAN);
    b.putLong(sequence).putLong(filePosition).putLong(readyOffset).flip();
    while (b.hasRemaining()) idx.write(b);
  }

  private static Path indexPath(Path log) {
    String name = log.getFileName().toString();
    return log.resolveSibling(name.substring(0, name.length() - 4) + ".idx");
  }

  private static void moveAtomic(Path from, Path to) throws IOException {
    try {
      Files.move(from, to, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (AtomicMoveNotSupportedException e) {
      Files.move(from, to, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private void apply(WalRecordType type, long seq, byte[] p) {
    switch (type) {
      case INGEST_RAW:
        AgentPublishRequest q = WalCodec.ingress(p);
        IngressAck ack = new IngressAck(q.getRequestId(), seq, false);
        requestIndex.put(q.idempotencyKey(), ack);
        requestChecksums.put(q.idempotencyKey(), q.getBatchChecksum());
        ingests.put(seq, new IngestRecord(seq, q));
        break;
      case EVENT_READY:
        WalCodec.Ready r = WalCodec.ready(p);
        StoredEvent s =
            new StoredEvent(
                r.event,
                StoredEvent.State.READY,
                Instant.EPOCH,
                null,
                0,
                "",
                r.configVersion,
                r.nodeId);
        ready.put(r.event.getOffset(), s);
        events.put(r.event.getEventId(), s);
        markProcessed(r.ingest, r.recordIndex);
        readyOffset = Math.max(readyOffset, r.event.getOffset());
        break;
      case PROCESS_REJECTED:
        WalCodec.Processed rejected = WalCodec.rejected(p);
        markProcessed(rejected.ingest, rejected.recordIndex);
        break;
      case DELIVERY_COMMIT:
        DeliveryCheckpoint cp = WalCodec.checkpoint(p);
        DeliveryCheckpoint old = checkpoints.get(cp.getChannelId());
        if (old == null || cp.getVersion() > old.getVersion())
          checkpoints.put(cp.getChannelId(), cp);
        break;
      case SEGMENT_SEAL:
        break;
      default:
        throw new IllegalStateException();
    }
  }

  private static String processedKey(long ingestSequence, int recordIndex) {
    return ingestSequence + ":" + recordIndex;
  }

  private void markProcessed(long ingestSequence, int recordIndex) {
    processedRecords.add(processedKey(ingestSequence, recordIndex));
  }

  private static void readFully(FileChannel c, ByteBuffer b, long pos) throws IOException {
    while (b.hasRemaining()) {
      int n = c.read(b, pos);
      if (n < 0) throw new EOFException();
      pos += n;
    }
  }

  @Override
  public void flush() {
    lock.lock();
    try {
      channel.force(true);
      indexChannel.force(true);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void close() {
    Thread join;
    lock.lock();
    try {
      if (channel == null) return;
      groupRunning = false;
      groupRequested.signalAll();
      groupCompleted.signalAll();
      join = groupThread;
    } finally {
      lock.unlock();
    }
    if (join != null && join != Thread.currentThread())
      try {
        join.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    lock.lock();
    try {
      if (channel != null) {
        channel.force(true);
        indexChannel.force(true);
        channel.close();
        indexChannel.close();
        channel = null;
        indexChannel = null;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      lock.unlock();
    }
  }

  public Path activePath() {
    return activePath;
  }
}
