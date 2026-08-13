package io.github.aicanal.cluster.raft;

import io.github.aicanal.cluster.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Election-only Raft group adapted from SOFAJRaft's election state transitions.
 *
 * <p>There is deliberately no replicated application log or state machine here. Persistent state is
 * limited to currentTerm and votedFor; the term is exposed as AI Canal's fencing epoch.
 */
final class RaftElectionGroup implements LeadershipHandle {
  private enum Role {
    FOLLOWER,
    CANDIDATE,
    LEADER,
    CLOSED
  }

  private final String groupId;
  private final RaftElectionConfig config;
  private final LeadershipListener listener;
  private final RaftSharedRuntime runtime;
  private final RaftMetaStore meta;
  private final int quorum;
  private final Set<String> preVotes = new HashSet<>(), votes = new HashSet<>();
  private final Map<String, Long> acknowledgements = new HashMap<>();
  private Role role = Role.FOLLOWER;
  private long currentTerm, prospectiveTerm, timerGeneration, leaderSinceNanos;
  private String votedFor, leaderId = "";
  private long lastLeaderContactNanos;
  private ScheduledFuture<?> electionTask, heartbeatTask;
  private Leadership leadership;

  RaftElectionGroup(
      String groupId,
      RaftElectionConfig config,
      LeadershipListener listener,
      RaftSharedRuntime runtime) {
    this.groupId = groupId;
    this.config = config;
    this.listener = listener;
    this.runtime = runtime;
    this.meta = new RaftMetaStore(config.dataDir, groupId);
    this.currentTerm = meta.term();
    this.votedFor = meta.votedFor();
    this.quorum = config.peers.size() / 2 + 1;
    this.lastLeaderContactNanos = System.nanoTime();
  }

  synchronized void start() {
    resetElectionTimer();
  }

  synchronized void onMessage(RaftMessage message) {
    if (role == Role.CLOSED || !config.peers.containsKey(message.senderId)) return;
    switch (message.type) {
      case RaftMessage.PRE_VOTE_REQUEST:
        onPreVoteRequest(message);
        break;
      case RaftMessage.PRE_VOTE_RESPONSE:
        onPreVoteResponse(message);
        break;
      case RaftMessage.VOTE_REQUEST:
        onVoteRequest(message);
        break;
      case RaftMessage.VOTE_RESPONSE:
        onVoteResponse(message);
        break;
      case RaftMessage.HEARTBEAT:
        onHeartbeat(message);
        break;
      case RaftMessage.HEARTBEAT_RESPONSE:
        onHeartbeatResponse(message);
        break;
      default:
        break;
    }
  }

  private void onPreVoteRequest(RaftMessage message) {
    boolean leaseValid = elapsedMillis(lastLeaderContactNanos) < config.electionTimeoutMillis;
    boolean granted = message.round >= currentTerm + 1 && !leaseValid;
    reply(message.senderId, RaftMessage.PRE_VOTE_RESPONSE, currentTerm, message.round, granted);
  }

  private void onPreVoteResponse(RaftMessage message) {
    if (role != Role.FOLLOWER || prospectiveTerm == 0) return;
    if (message.term > currentTerm) {
      updateTerm(message.term);
      prospectiveTerm = 0;
      preVotes.clear();
      resetElectionTimer();
      return;
    }
    if (message.round != prospectiveTerm || !message.granted) return;
    preVotes.add(message.senderId);
    if (preVotes.size() >= quorum) beginElection();
  }

  private void onVoteRequest(RaftMessage message) {
    if (message.term > currentTerm) stepDown(message.term);
    boolean granted =
        message.term == currentTerm && (votedFor.isEmpty() || votedFor.equals(message.senderId));
    if (granted) {
      if (!votedFor.equals(message.senderId)) persist(currentTerm, message.senderId);
      votedFor = message.senderId;
      leaderId = "";
      lastLeaderContactNanos = System.nanoTime();
      resetElectionTimer();
    }
    reply(message.senderId, RaftMessage.VOTE_RESPONSE, currentTerm, 0, granted);
  }

  private void onVoteResponse(RaftMessage message) {
    if (message.term > currentTerm) {
      stepDown(message.term);
      return;
    }
    if (role != Role.CANDIDATE || message.term != currentTerm || !message.granted) return;
    votes.add(message.senderId);
    if (votes.size() >= quorum) becomeLeader();
  }

  private void onHeartbeat(RaftMessage message) {
    if (message.term < currentTerm) {
      reply(message.senderId, RaftMessage.HEARTBEAT_RESPONSE, currentTerm, 0, false);
      return;
    }
    if (message.term > currentTerm || role != Role.FOLLOWER) stepDown(message.term);
    leaderId = message.senderId;
    prospectiveTerm = 0;
    preVotes.clear();
    lastLeaderContactNanos = System.nanoTime();
    resetElectionTimer();
    reply(message.senderId, RaftMessage.HEARTBEAT_RESPONSE, currentTerm, 0, true);
  }

  private void onHeartbeatResponse(RaftMessage message) {
    if (message.term > currentTerm) {
      stepDown(message.term);
      return;
    }
    if (role == Role.LEADER && message.term == currentTerm && message.granted)
      acknowledgements.put(message.senderId, System.nanoTime());
  }

  private void onElectionTimeout(long generation) {
    synchronized (this) {
      if ((role != Role.FOLLOWER && role != Role.CANDIDATE) || generation != timerGeneration)
        return;
      if (role == Role.CANDIDATE) role = Role.FOLLOWER;
      if (!leaderId.isEmpty()
          && elapsedMillis(lastLeaderContactNanos) < config.electionTimeoutMillis) {
        resetElectionTimer();
        return;
      }
      leaderId = "";
      prospectiveTerm = currentTerm + 1;
      preVotes.clear();
      preVotes.add(config.nodeId);
      broadcast(RaftMessage.PRE_VOTE_REQUEST, currentTerm, prospectiveTerm, false);
      if (preVotes.size() >= quorum) beginElection();
      else resetElectionTimer();
    }
  }

  private void beginElection() {
    if (role == Role.CLOSED) return;
    role = Role.CANDIDATE;
    currentTerm = Math.max(currentTerm + 1, prospectiveTerm);
    persist(currentTerm, config.nodeId);
    votedFor = config.nodeId;
    leaderId = "";
    votes.clear();
    votes.add(config.nodeId);
    broadcast(RaftMessage.VOTE_REQUEST, currentTerm, 0, false);
    if (votes.size() >= quorum) becomeLeader();
    else resetElectionTimer();
  }

  private void becomeLeader() {
    if (role != Role.CANDIDATE) return;
    role = Role.LEADER;
    leaderId = config.nodeId;
    cancel(electionTask);
    electionTask = null;
    long now = System.nanoTime();
    leaderSinceNanos = now;
    acknowledgements.clear();
    for (String voter : votes) acknowledgements.put(voter, now);
    acknowledgements.put(config.nodeId, now);
    leadership = new Leadership(groupId, config.nodeId, currentTerm);
    listener.onAcquired(leadership);
    scheduleHeartbeat(0);
  }

  private void heartbeatTick(long generation) {
    synchronized (this) {
      if (role != Role.LEADER || generation != timerGeneration) return;
      long now = System.nanoTime();
      if (elapsedMillis(leaderSinceNanos) >= leaderLeaseMillis() && aliveVoters(now) < quorum) {
        stepDown(currentTerm);
        return;
      }
      acknowledgements.put(config.nodeId, now);
      broadcast(RaftMessage.HEARTBEAT, currentTerm, 0, false);
      scheduleHeartbeat(config.heartbeatIntervalMillis);
    }
  }

  private int aliveVoters(long now) {
    int alive = 0;
    long limit = TimeUnit.MILLISECONDS.toNanos(leaderLeaseMillis());
    for (String peer : config.peers.keySet()) {
      Long acknowledged = acknowledgements.get(peer);
      if (acknowledged != null && now - acknowledged <= limit) alive++;
    }
    return alive;
  }

  private void stepDown(long term) {
    if (term > currentTerm) {
      currentTerm = term;
      persist(currentTerm, "");
      votedFor = "";
    }
    if (leadership != null) {
      Leadership previous = leadership;
      leadership = null;
      listener.onRevoked(previous);
    }
    role = Role.FOLLOWER;
    leaderId = "";
    prospectiveTerm = 0;
    preVotes.clear();
    votes.clear();
    cancel(heartbeatTask);
    heartbeatTask = null;
    lastLeaderContactNanos = System.nanoTime();
    resetElectionTimer();
  }

  private void updateTerm(long term) {
    if (term <= currentTerm) return;
    currentTerm = term;
    persist(term, "");
    votedFor = "";
  }

  private void persist(long term, String vote) {
    meta.save(term, vote);
  }

  private void broadcast(byte type, long term, long round, boolean granted) {
    RaftMessage message =
        new RaftMessage(type, config.clusterId, groupId, config.nodeId, term, round, granted);
    for (RaftPeer peer : config.peers.values())
      if (!peer.nodeId.equals(config.nodeId)) runtime.send(peer, message);
  }

  private void reply(String nodeId, byte type, long term, long round, boolean granted) {
    RaftPeer peer = config.peers.get(nodeId);
    if (peer != null)
      runtime.send(
          peer,
          new RaftMessage(type, config.clusterId, groupId, config.nodeId, term, round, granted));
  }

  private void resetElectionTimer() {
    if (role == Role.CLOSED || role == Role.LEADER) return;
    cancel(electionTask);
    long generation = ++timerGeneration;
    int jitter = ThreadLocalRandom.current().nextInt(config.electionTimeoutMillis);
    electionTask =
        runtime.schedule(
            () -> safely(() -> onElectionTimeout(generation)),
            config.electionTimeoutMillis + jitter);
  }

  private void scheduleHeartbeat(long delayMillis) {
    cancel(heartbeatTask);
    long generation = ++timerGeneration;
    heartbeatTask = runtime.schedule(() -> safely(() -> heartbeatTick(generation)), delayMillis);
  }

  private void safely(Runnable task) {
    try {
      task.run();
    } catch (Throwable failure) {
      fail(failure);
    }
  }

  synchronized void fail(Throwable failure) {
    if (role == Role.CLOSED) return;
    if (leadership != null) {
      listener.onRevoked(leadership);
      leadership = null;
    }
    role = Role.CLOSED;
    cancel(electionTask);
    cancel(heartbeatTask);
    runtime.unregister(groupId, this);
    runtime.reportFailure(groupId, failure);
  }

  private static void cancel(Future<?> task) {
    if (task != null) task.cancel(false);
  }

  private static long elapsedMillis(long startedNanos) {
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
  }

  private long leaderLeaseMillis() {
    return Math.max(config.heartbeatIntervalMillis * 2L, config.electionTimeoutMillis * 2L / 3L);
  }

  @Override
  public synchronized void close() {
    if (role == Role.CLOSED) return;
    if (leadership != null) {
      listener.onRevoked(leadership);
      leadership = null;
    }
    role = Role.CLOSED;
    cancel(electionTask);
    cancel(heartbeatTask);
    runtime.unregister(groupId, this);
  }
}
