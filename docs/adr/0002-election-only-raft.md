# ADR-0002：按 destination 的 election-only Raft

- 状态：已接受
- 日期：2026-08-13

## 背景

现有 `LeaderElector`、`LeadershipListener` 和 `LeaderGuard` 已经把选主与业务流水线解耦。系统的数据副本由 Agent FANOUT 写入各节点本地 WAL，集群协调层不承担业务数据复制，因此直接引入完整 Raft 日志、状态机和快照会改变既有数据架构并增加运行开销。

## 决策

1. 新增独立 `canal-cluster-raft` 模块，实现 `LeaderElector` SPI；`standalone` 和 `zookeeper` 行为保持不变。
2. 一个 destination 对应一个 Raft group，所有 group 使用相同静态 voter 集合，但共享进程级 UDP transport、定时器和 RPC 线程池。
3. 只实现选主所需的 pre-vote、RequestVote、心跳、随机超时和 check-quorum；持久化状态只有 `currentTerm` 与 `votedFor`。
4. Raft term 映射为既有 `Leadership.epoch`。业务供数、MQ worker 和 checkpoint CAS 继续通过原有 `LeaderGuard` fencing。
5. Raft 不复制 WAL，不改变 Agent FANOUT、at-least-once、TCP/MQ 出口或配置发布语义。
6. 成员变更采用静态配置和运维重启；当前不实现 joint-consensus。

## 性能与故障边界

每个 destination 只增加小型状态对象和定时任务，不创建 socket 或专属线程。Leader 失去多数派时在租约窗口内主动降级；任期和投票持久化失败时 group fail-closed 并撤销领导权。UDP transport 依赖可信内网、稳定的 peer 地址和网络层访问控制。

## 参考与许可

选举状态转换参考本地 SOFAJRaft 1.4.0 实现，未引入其日志复制、状态机、快照或存储代码。SOFAJRaft 由 Ant Group 以 Apache License 2.0 发布；归属说明见 `canal-cluster-raft/NOTICE`。
