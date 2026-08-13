package io.github.aicanal.cluster.raft;

import java.io.IOException;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.*;

final class RaftSharedRuntime implements AutoCloseable {
  private static final Logger LOG = Logger.getLogger(RaftSharedRuntime.class.getName());
  private final RaftElectionConfig config;
  private final DatagramSocket socket;
  private final ScheduledExecutorService scheduler;
  private final ExecutorService rpcWorkers;
  private final Map<String, RaftElectionGroup> groups = new ConcurrentHashMap<>();
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final Thread receiver;

  RaftSharedRuntime(RaftElectionConfig config) {
    this.config = config;
    try {
      socket = new DatagramSocket(config.bindAddress);
      socket.setReceiveBufferSize(1024 * 1024);
      socket.setSendBufferSize(1024 * 1024);
    } catch (SocketException e) {
      throw new IllegalStateException(
          "cannot bind raft election transport " + config.bindAddress, e);
    }
    scheduler =
        Executors.newScheduledThreadPool(
            2,
            runnable -> {
              Thread thread = new Thread(runnable, "raft-election-timer-" + config.nodeId);
              thread.setDaemon(true);
              return thread;
            });
    rpcWorkers =
        Executors.newFixedThreadPool(
            config.rpcThreads,
            runnable -> {
              Thread thread = new Thread(runnable, "raft-election-rpc-" + config.nodeId);
              thread.setDaemon(true);
              return thread;
            });
    receiver = new Thread(this::receiveLoop, "raft-election-receiver-" + config.nodeId);
    receiver.setDaemon(true);
    receiver.start();
  }

  RaftElectionGroup register(
      String groupId, io.github.aicanal.cluster.LeadershipListener listener) {
    RaftElectionGroup group = new RaftElectionGroup(groupId, config, listener, this);
    if (groups.putIfAbsent(groupId, group) != null)
      throw new IllegalStateException("duplicate raft election group " + groupId);
    group.start();
    return group;
  }

  void unregister(String groupId, RaftElectionGroup expected) {
    groups.remove(groupId, expected);
  }

  ScheduledFuture<?> schedule(Runnable task, long delayMillis) {
    return scheduler.schedule(task, delayMillis, TimeUnit.MILLISECONDS);
  }

  void send(RaftPeer peer, RaftMessage message) {
    if (!running.get()) return;
    byte[] bytes = message.encode();
    DatagramPacket packet = new DatagramPacket(bytes, bytes.length, peer.address);
    try {
      socket.send(packet);
    } catch (IOException ignored) {
      // UDP loss is handled by randomized election retries and periodic heartbeats.
    }
  }

  private void receiveLoop() {
    byte[] buffer = new byte[4096];
    while (running.get()) {
      DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
      try {
        socket.receive(packet);
        RaftMessage message = RaftMessage.decode(packet.getData(), packet.getLength());
        if (!config.clusterId.equals(message.clusterId) || !validSource(message, packet)) continue;
        RaftElectionGroup group = groups.get(message.groupId);
        if (group != null)
          rpcWorkers.execute(
              () -> {
                try {
                  group.onMessage(message);
                } catch (Throwable failure) {
                  group.fail(failure);
                }
              });
      } catch (SocketException e) {
        if (running.get()) continue;
        return;
      } catch (IOException | RuntimeException ignored) {
        // Malformed and unknown datagrams are rejected without affecting election state.
      }
    }
  }

  private boolean validSource(RaftMessage message, DatagramPacket packet) {
    RaftPeer peer = config.peers.get(message.senderId);
    if (peer == null || peer.address.getPort() != packet.getPort()) return false;
    InetAddress expected = peer.address.getAddress();
    return expected == null || expected.equals(packet.getAddress());
  }

  void reportFailure(String groupId, Throwable failure) {
    LOG.log(Level.SEVERE, "raft election group failed closed: " + groupId, failure);
  }

  @Override
  public void close() {
    if (!running.compareAndSet(true, false)) return;
    for (RaftElectionGroup group : new ArrayList<>(groups.values())) group.close();
    groups.clear();
    socket.close();
    scheduler.shutdownNow();
    rpcWorkers.shutdownNow();
    if (receiver != Thread.currentThread())
      try {
        receiver.join(2000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
  }
}
