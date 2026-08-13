package io.github.aicanal.cluster.zookeeper;

import io.github.aicanal.cluster.*;
import io.github.aicanal.spi.PluginContext;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.apache.curator.framework.*;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

public final class ZookeeperLeaderElector implements LeaderElector {
  private CuratorFramework client;
  private String nodeId, root;
  private final List<Handle> handles = new CopyOnWriteArrayList<>();

  public String type() {
    return "zookeeper";
  }

  public void validate(Map<String, Object> c) {
    if (!c.containsKey("connectString"))
      throw new IllegalArgumentException("zookeeper requires connectString");
  }

  public void initialize(PluginContext p, Map<String, Object> c) {
    nodeId = p.getNodeId();
    root = "/" + String.valueOf(c.getOrDefault("namespace", "ai-canal"));
    client =
        CuratorFrameworkFactory.newClient(
            String.valueOf(c.get("connectString")),
            15000,
            5000,
            new ExponentialBackoffRetry(1000, 5));
    client
        .getConnectionStateListenable()
        .addListener(
            (ignored, state) -> {
              if (state == ConnectionState.SUSPENDED
                  || state == ConnectionState.LOST
                  || state == ConnectionState.READ_ONLY) for (Handle h : handles) h.revokeNow();
            });
  }

  public void start() {
    client.start();
    try {
      if (!client.blockUntilConnected(10, TimeUnit.SECONDS))
        throw new IllegalStateException("ZooKeeper unavailable");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  public LeadershipHandle participate(String d, LeadershipListener l) {
    try {
      Handle h = new Handle(d, l);
      handles.add(h);
      h.start();
      return h;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private long nextEpoch(String d) throws Exception {
    String path = root + "/destinations/" + d + "/epoch";
    try {
      client.create().creatingParentsIfNeeded().forPath(path, "0".getBytes(StandardCharsets.UTF_8));
    } catch (KeeperException.NodeExistsException ignored) {
    }
    for (; ; ) {
      org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
      long old =
          Long.parseLong(
              new String(
                  client.getData().storingStatIn(stat).forPath(path), StandardCharsets.UTF_8));
      long next = old + 1;
      try {
        client
            .setData()
            .withVersion(stat.getVersion())
            .forPath(path, String.valueOf(next).getBytes(StandardCharsets.UTF_8));
        return next;
      } catch (KeeperException.BadVersionException retry) {
      }
    }
  }

  public void close() {
    for (Handle h : handles) h.close();
    handles.clear();
    if (client != null) client.close();
  }

  private final class Handle implements LeadershipHandle, Runnable {
    final String destination;
    final LeadershipListener listener;
    final LeaderLatch latch;
    final ExecutorService loop = Executors.newSingleThreadExecutor();
    volatile boolean open = true;
    volatile Leadership current;

    Handle(String d, LeadershipListener l) throws Exception {
      destination = d;
      listener = l;
      latch = new LeaderLatch(client, root + "/destinations/" + d + "/candidates", nodeId);
    }

    void start() throws Exception {
      latch.start();
      loop.submit(this);
    }

    public void run() {
      while (open) {
        try {
          latch.await();
          if (!open) return;
          Leadership acquired = new Leadership(destination, nodeId, nextEpoch(destination));
          current = acquired;
          listener.onAcquired(acquired);
          while (open && latch.hasLeadership() && current != null) Thread.sleep(100);
          revokeNow();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return;
        } catch (Exception e) {
          revokeNow();
          try {
            Thread.sleep(500);
          } catch (InterruptedException x) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    }

    synchronized void revokeNow() {
      Leadership previous = current;
      if (previous != null) {
        current = null;
        listener.onRevoked(previous);
      }
    }

    public void close() {
      open = false;
      revokeNow();
      try {
        latch.close();
      } catch (IOException ignored) {
      }
      loop.shutdownNow();
    }
  }
}
