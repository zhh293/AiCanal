# AI Canal 的 Election-Only Raft：选主逻辑、脑裂防护与「供数 vs 日志复制」的取舍

- 状态：已接受
- 日期：2026-08-14
- 关联决策：[ADR-0001 第一版实现基线](docs/adr/0001-baseline-decisions.md)、[ADR-0002 按 destination 的 election-only Raft](docs/adr/0002-election-only-raft.md)
- 关联代码：`canal-cluster-raft/`、`canal-cluster-api/`、`canal-storage-default/`、`canal-egress-netty/`

---

## 摘要（结论先行）

AI Canal 的 Raft 模块是 **election-only** 的：它只做「选主 + fencing」，**不复制业务日志、不维护状态机、不做快照**。这带来一个自然的疑问——

> 「标准 Raft 是靠『日志复制 + 多数派提交』来防脑裂双写的，砍掉日志之后，凭什么还能保证同一时刻只有一个节点供数？」

本文要论证的核心结论是：

1. **防脑裂（Election Safety）从来就不依赖日志**，它依赖「多数派投票 + 任期（term）+ 一任期一票」。这三样在 election-only 实现里**完整保留**。
2. **日志在标准 Raft 里防的是「丢已提交数据」（Leader Completeness），不是「两个 leader 同时写」**。这个项目用 **Agent FANOUT** 替代了日志复制的这个作用。
3. **「供数」在这个系统里是「读」，不是「写」**。真正需要跨节点多数派一致的可变状态只有一个——**「谁有供数权」**，而这正是 Raft 选举锁死的东西。
4. 代价是：脑裂的 lease 窗口内可能**重复投递**（不是数据混乱/丢失），由下游按 `eventId` 幂等兜底。

一句话：**「多数派确认」没有被删掉，它被精确地缩小到了「供数权」这一个共享状态上。**

---

## 1. 背景与问题

系统要满足的约束（来自 README 与 ADR）：

- 同一 `destination` 在同一时刻**只能有一个节点是 Leader 并对下游供数**；
- 数据副本的一致性由 **Agent FANOUT** 保证：Agent 把相同的 `requestId` 和相同内容推给该 destination 的**所有**候选节点，每个节点独立写自己的本地 WAL；
- 集群协调层（ZooKeeper 或 Raft）**只负责选主和 fencing，不复制 WAL 数据**。

因此引入一个矛盾点：标准 Raft 的「日志」承担了两个职责，而本项目只需要其中一个。厘清这两个职责，是理解整套设计的前提。

---

## 2. 标准 Raft 选主逻辑回顾（论文基线）

先回到论文。一次选举的完整流程是：

1. **随机选举计时器**：每个 Follower 启动一个随机超时（例如 150–300ms 内的随机值）。随机化是为了**避免多个节点同时超时、同时发起选举**。
2. **心跳重置**：Leader 周期性发心跳（`AppendEntries` 空日志）。Follower 收到有效心跳就重置计时器。
3. **超时触发选举**：计时器到期仍未收到心跳 → 转为 Candidate，`term++`，投自己一票，并行向其他节点发 `RequestVote`。
4. **投票标准**：接收方按两条规则决定是否投票：
   - **任期比较**：候选人的 `term` 比自己的 `currentTerm` 小 → 直接拒绝；比自己的大 → 先更新自己的 term（step down）再考虑。
   - **日志新旧比较**：候选人的日志必须「至少和自己一样新」。具体是「最后一条日志的 term 更大」或「term 相同但 index 更大」，否则拒绝。这是 **Leader Completeness** 的保证。
5. **过半晋升**：Candidate 拿到多数派票 → 成为 Leader，通过心跳把新 term 广播给所有节点。
6. **上任第一件事**：Leader 先追加一条**当前任期的 no-op 空日志**并复制到多数派。这是为了「提交之前任期遗留的日志」——因为 Leader 不能直接提交旧任期的日志，必须借助一条新任期日志的提交来间接提交它们。
7. **日志同步**：此后 Leader 通过 `AppendEntries` 携带 `prevLogIndex/prevLogTerm` 同步日志，Follower 不一致时按一致性检查回退补齐（即「三种情况」的分支，本文略）。

> 说明：**pre-vote（预投票）并非 Raft 原始论文的一部分**，是后来社区为缓解「分区节点不断自增 term 导致任期飙升」而加的扩展。本项目实现了 pre-vote（见 §3.1）。

---

## 3. AI Canal 的 Raft 实现

### 3.1 选主状态机与代码对应

`canal-cluster-raft/src/main/java/io/github/aicanal/cluster/raft/RaftElectionGroup.java` 实现的就是 §2 的状态机，逐条对应如下：

| 论文逻辑 | 项目实现 | 代码位置 |
| --- | --- | --- |
| 随机选举计时器 | `resetElectionTimer()` 用 `ThreadLocalRandom` 在 `electionTimeoutMillis` 上加 jitter | `RaftElectionGroup.java:272-281` |
| 心跳重置计时器 | `onHeartbeat()` 里 `lastLeaderContactNanos = now` 并 `resetElectionTimer()` | `RaftElectionGroup.java:127-139` |
| 超时先「喊一嗓子」（pre-vote） | `onElectionTimeout()` 先广播 `PRE_VOTE_REQUEST`，`preVotes.size() >= quorum` 才 `beginElection()` | `RaftElectionGroup.java:150-168` |
| 投票看任期 | `onVoteRequest()` 里 `message.term == currentTerm` 才可能投 | `RaftElectionGroup.java:103-115` |
| 一节点一任期一票 | `votedFor` 持久化，`RaftMetaStore.save()` 硬校验「同任期不能投两次」 | `RaftMetaStore.java:28-58` |
| 过半晋升 Leader | `onVoteResponse()` 里 `votes.size() >= quorum` → `becomeLeader()` | `RaftElectionGroup.java:117-125` |
| ~~比较日志新旧~~ | **没有这一栏**，`onVoteRequest` 里除 term 外无其他投票条件 | `RaftElectionGroup.java:105-106` |
| ~~no-op 空日志 / 日志同步~~ | **没有**，无日志可同步 | — |

**唯一被移除的是「日志比较」与「日志同步」**，这正是 election-only 的取舍所在。

### 3.2 持久化状态：只有 `currentTerm` 与 `votedFor`

`RaftMetaStore.java` 持久化到 `data/raft-election/{destination}/meta.properties`，只有两个字段：

```text
term=3
votedFor=canal-1
```

两个关键约束在 `save()` 里被硬校验：

- **term 不能倒退**：`newTerm < term` 直接抛异常（`RaftMetaStore.java:29`）；
- **一任期不能投两票**：同 term 下若 `votedFor` 非空且与新的不同，抛异常（`RaftMetaStore.java:30-34`）。

写文件采用「写临时文件 + `force(true)` + 原子 rename」，保证落盘可靠（`RaftMetaStore.java:38-53`）。

### 3.3 与其他选主模式的对比

| 模式 | 数据复制 | 选主机制 | 持久化 |
| --- | --- | --- | --- |
| `standalone` | 无（单机） | 恒定 leader | 无 |
| `zookeeper` | 无（Agent FANOUT） | Curator LeaderLatch + epoch CAS | ZK 节点 |
| `raft` | 无（Agent FANOUT） | 本文的 election-only Multi-Raft | 本地 `term`/`votedFor` |

三种模式的**数据复制模型完全一致（Agent FANOUT）**，区别只在「选主 + fencing」这一控制面如何实现。

---

## 4. 两个安全性质：为什么「没日志」不等于「没防脑裂」

这是理解整套设计最关键的掰扯。Raft 的安全性由多个不变量共同保证，其中两个最容易被混为一谈：

### 4.1 Election Safety（防脑裂）

> 同一任期（term）内，最多只能有一个 Leader。

它由两条规则保证：

1. **一节点一任期一票**（`votedFor` 持久化，§3.2）；
2. **当选需要多数派**（`quorum = n/2 + 1`）。

因为任意两个多数派集合必然相交（`n/2+1` 和 `n/2+1` 加起来大于 `n`），而相交的那个节点在同一任期不能投两次票，所以**同一任期不可能选出两个 leader**。

**这两条规则与日志毫无关系。**

### 4.2 Leader Completeness（防丢已提交数据）

> 新 Leader 必须包含所有「已经提交（committed）到多数派」的日志条目。

这是投票里「比较日志新旧」的作用：只让「日志最全」的节点当选，避免落后节点当选后把已提交数据覆盖掉。它防的是**数据丢失**，不是**两个 leader**。

### 4.3 关键结论

| 安全性质 | 防什么 | 靠什么 | 本项目是否需要 |
| --- | --- | --- | --- |
| Election Safety | 同一任期两个 leader | 多数派 + term + 一任期一票 | ✅ 需要（保留） |
| Leader Completeness | 丢已提交日志 | 日志新旧比较 + 日志复制 | ❌ 不需要（由 Agent FANOUT 替代） |

所以「election-only」是一个**精确地只删掉 Leader Completeness、完整保留 Election Safety** 的设计。它砍掉的不是「防脑裂」，而是「防丢数据」，而后者被 Agent FANOUT 接管了——因为每个节点在数据面就已经有全量副本，谁当选谁都不缺数据。

---

## 5. 没有日志，脑裂到底怎么防：三层防线

回到那个担忧：「如果只靠节点自己判断『我是 leader 还是 follower』来决定供不供数，少数派里的老 leader 会一直自我感觉良好、继续供数」。项目实际有三层防线，不是只靠自判：

### 5.1 第一层：多数派投票本身

`quorum = config.peers.size() / 2 + 1`（`RaftElectionGroup.java:49`）。脑裂后只有**多数派那一侧**能凑够票选出新 leader，少数派那一侧数学上不可能选出第二个 leader。

### 5.2 第二层：check-quorum 主动退位

这是最关键、也最容易被忽略的一层。少数派里的老 leader 收不到多数派的心跳 ACK，`heartbeatTick()` 会检测（`RaftElectionGroup.java:200-211`）：

```java
if (elapsedMillis(leaderSinceNanos) >= leaderLeaseMillis() && aliveVoters(now) < quorum) {
    stepDown(currentTerm);   // 主动放弃领导权
    return;
}
```

`aliveVoters()` 统计最近一个 lease 内有多少节点 ACK 了心跳，**不足多数派就自己退位**。

注意它检测的不是「我是不是 leader」，而是「我还持不持有多数派」——这直接化解了「自我感觉良好」的问题：一个失去多数派的 leader 会在 lease 窗口内**主动**降级，不用等分区恢复。

### 5.3 第三层：epoch fencing（最后兜底）

Raft 的 `term` 被直接映射成 `Leadership.epoch`（`RaftElectionGroup.java:195`）：

```java
leadership = new Leadership(groupId, config.nodeId, currentTerm);
```

下游供数和 checkpoint 全部带 epoch：

- TCP 消费者 `TcpSubscription.require()` 每次 `FETCH`/`ACK` 都校验 `guard.isLeader(destination, epoch)`（`TcpSubscription.java:44-46`）；
- `SegmentedWalEventStore.commitDelivery()` 里 `cur.getLeaderEpoch() != epoch` 直接抛 `CheckpointConflictException`（`SegmentedWalEventStore.java:262`）。

这意味着：**即使老 leader 在 lease 窗口内没来得及退位、还在供数，它提交 checkpoint 时 epoch 是旧的，CAS 会被拒绝**。而且分区恢复后，新 leader 更高 term 的心跳一到，`onHeartbeat` 里 `message.term > currentTerm` 会让老 leader 立即 `stepDown`（`RaftElectionGroup.java:127-139`）。

三层的关系是：**多数派**从数学上保证「不会有两个合法 leader」；**check-quorum** 让失去多数派的 leader 尽快自我降级；**epoch fencing** 给下游和 checkpoint 一道不依赖「leader 是否及时自觉」的硬闸。

---

## 6. 核心辨析：「供数」是读，不是写

争论里最需要掰开的一点，是「把供数类比成写日志」这一步。这个类比不成立，而它恰恰是理解取舍的钥匙。

### 6.1 数据写入发生在哪

在 AI Canal 里，**数据写入（WAL append）在 Agent FANOUT 阶段就已经完成了**。Agent 把相同内容推到所有节点，每个节点独立地：

```
Agent PUBLISH → 写 INGEST_RAW → 流水线 parse/classify/dedup/audit → 写 EVENT_READY
```

这一步是**本地**的，不涉及任何跨节点状态变更。

### 6.2 供数是「纯读 + 投递」

等消费者来取数时，节点做的动作是：

```
读本地 WAL 里已经处理好的 EVENT_READY → 投递给下游
```

这是**读操作 + 网络投递**，不改变任何跨节点状态。所以「供数需要多数派确认」在这个架构里**没有对应的东西可以确认**——不存在一个需要达成一致的跨节点写。

### 6.3 真正需要多数派的只有一个状态

对比标准 Raft：

- 标准 Raft 里需要多数派确认的是**「日志」这个共享状态的变更**；
- AI Canal 里数据状态的变更（写 WAL）已经由 FANOUT 在数据面完成；
- 集群层真正需要多数派一致的共享可变状态**只有一个：谁有供数权**。

于是「多数派确认」没有被删掉，而是被**精确地缩小到「供数权」这一个点**上：

| 需要多数派的东西 | 谁保证 |
| --- | --- |
| 谁是 leader（供数权） | Raft 选举：`quorum = n/2+1` + 一任期一票 |
| 数据副本一致性 | Agent FANOUT（数据面，不靠 Raft） |
| 防「两个 leader 双写」 | 供数权多数派 + epoch fencing + checkpoint 本地化 |

因为**供数权是唯一的共享可变状态**，只要它被多数派锁死，就不会有「两个节点同时拥有合法供数权」。而数据本身因为 FANOUT 每个节点都有全量副本，「谁能供数」和「数据对不对」就彻底解耦了。

---

## 7. 两种架构路线的对比

争论中提出的「每供一次要多数派收到并确认」其实指向**另一条架构路线**。

### 7.1 路线 A：同步复制 / 多数派确认供数

即把「供数」变成一次多数派提交（quorum write）：

- Kafka `acks=all`（每次 produce 等多数 ISR 确认）；
- Raft/etcd 状态机（每次写要多数派 commit）；
- 数据库同步复制。

**优点**：能像争论中设想的那样，从根上消灭「脑裂窗口内的双写」。

**代价**：每次供数都要跨网络等多数派 ACK，**延迟和吞吐严重恶化**。供数是高频热路径，把它变成同步复制，等于拿掉这个系统「本地读、低延迟供数」的核心优势。

### 7.2 路线 B：Agent FANOUT + at-least-once + 下游幂等（本项目）

数据面异步复制，控制面只用多数派锁住供数权，供数本身是本地读。

- **优点**：供数延迟低、吞吐高；脑裂由供数权多数派 + check-quorum + epoch fencing 控制。
- **代价**：脑裂 lease 窗口内可能**重复投递**，需要下游按 `eventId` 幂等消化。

ADR-0002 明确选了路线 B：

> 「只实现选主所需的 pre-vote、RequestVote、心跳、随机超时和 check-quorum……Raft 不复制 WAL，不改变 Agent FANOUT、at-least-once 语义。」

### 7.3 对比表

| 维度 | 路线 A：同步复制 | 路线 B：FANOUT + 幂等（本项目） |
| --- | --- | --- |
| 供数延迟 | 高（等多数派 ACK） | 低（本地读） |
| 供数吞吐 | 受限于跨网络确认 | 高 |
| 脑裂双写 | 从根上消灭 | lease 窗口内可能重复投递 |
| 一致性语义 | 强一致 / exactly-once 倾向 | at-least-once + 下游幂等 |
| 运维复杂度 | 同步复制协议、写放大 | Agent 必须 FANOUT 到所有节点 |
| 适用场景 | 需要强一致的写 | 数据流、可幂等消费 |

这是一个经典的**「用最终一致 + 幂等换低延迟」**的权衡，与 Kafka（默认 `acks=1` 也可以配 `acks=all`）、Canal 原版等系统的取向一致。

---

## 8. 决策记录（ADR 风格）

**决策**：采用 election-only Raft，保留 Election Safety（多数派 + term + 一任期一票 + check-quorum + epoch fencing），移除 Leader Completeness（日志复制），由 Agent FANOUT 承担数据副本一致性。

**理由**：

1. 系统数据架构已定：Agent FANOUT 让每个节点在数据面就有全量副本，集群层再引入日志复制会造成重复且增加运行开销（ADR-0002）。
2. 「供数」是读操作，不存在需要跨节点多数派一致的数据写；唯一共享可变状态是供数权，用选举即可锁死。
3. 同步复制会摧毁本地读的低延迟优势，与「资源接入/分发」的数据流定位不符。

**代价（已接受）**：

1. 脑裂 lease 窗口内可能重复投递，下游必须按 `eventId` 幂等。
2. 运维上 Agent 必须把相同 `requestId` + 内容推给所有节点；否则 FANOUT 失效，节点数据不齐。
3. 静态成员变更需运维重启，不支持在线 joint-consensus（ADR-0002）。

---

## 9. 残留的代价与边界

### 9.1 脑裂窗口内的重复投递

三层防线把风险压缩但**没有消灭**。具体窗口：

- 老 leader 失去多数派 → check-quorum 要等 `leaderLeaseMillis = max(heartbeatInterval*2, electionTimeout*2/3)`（`RaftElectionGroup.java:318-320`）到期才退位；
- 这段 lease 窗口内，老 leader 的 `isLeader` 仍为 true，**仍会往下游供数**（TCP 返回 `DATA_BATCH` / MQ 已 `send`）；
- 同时多数派已选出新 leader 也在供数 → **两个节点短暂同时发数据**。

但注意：这表现为**重复投递**，不是 checkpoint 错乱——因为 Agent FANOUT 下两个节点各有各的本地 WAL 和独立 checkpoint（`tcp:consumerX` 各自独立），不会互相覆盖。下游靠 `eventId` 幂等消化这份重复。这正是 README 反复强调「at-least-once、下游必须幂等」的根因。

### 9.2 运维约束：Agent FANOUT

- Agent 必须知道该 destination 的**所有**候选节点；
- 对每个副本使用**相同的 `requestId` 和内容**；
- 每个节点使用**独立持久化的数据卷**。

任何一条不满足，多节点数据一致性就不成立。

### 9.3 下游责任：幂等

下游必须把 `eventId` 当作幂等键，接受 at-least-once 的重复窗口。这是系统把「一部分正确性压力」外移给消费端的明确约定。

---

## 10. 结论

1. **「没日志不等于没防脑裂」**。防脑裂靠「多数派 + term + 一任期一票 + check-quorum + epoch fencing」，这些在 election-only 实现里**全部保留**。
2. **日志防的是「丢已提交数据」（Leader Completeness），不是「两个 leader 双写」**。这个职责由 Agent FANOUT 替代。
3. **「供数」是读，不是写**。真正需要多数派一致的共享状态只有「供数权」，Raft 选举锁死了它。
4. **「供数走多数派确认」是另一条更重的同步复制路线**，能消灭重复，但会摧毁低延迟供数。本项目明确选择了 FANOUT + at-least-once + 下游幂等。
5. **最终代价只有一个**：脑裂 lease 窗口内可能重复投递，由下游 `eventId` 幂等兜底——这是「用最终一致换低延迟」的经典权衡，而非「防脑裂失效」。

如果目标是「供数本身也强一致、绝不重复」，那确实需要走同步复制路线；但那是一个完全不同、也更重的系统。

---

## 附：关键代码位置索引

| 主题 | 文件 | 位置 |
| --- | --- | --- |
| 选主状态机 | `canal-cluster-raft/.../RaftElectionGroup.java` | 全文 |
| quorum 定义 | 同上 | `:49` |
| pre-vote | 同上 | `:83-101`、`:150-168` |
| 投票（仅 term） | 同上 | `:103-115` |
| check-quorum 主动退位 | 同上 | `:200-211` |
| stepDown | 同上 | `:224-244` |
| 随机计时器 | 同上 | `:272-281` |
| leader lease 时长 | 同上 | `:318-320` |
| term → epoch | 同上 | `:195` |
| term/votedFor 持久化 | `canal-cluster-raft/.../RaftMetaStore.java` | `:28-58` |
| epoch fencing（TCP 供数） | `canal-egress-netty/.../TcpSubscription.java` | `:44-46` |
| epoch fencing（checkpoint CAS） | `canal-storage-default/.../SegmentedWalEventStore.java` | `:252-273` |
| 决策背景 | `docs/adr/0002-election-only-raft.md` | 全文 |
| 数据复制基线说明 | `README.md` | 「多节点模型」一节 |
