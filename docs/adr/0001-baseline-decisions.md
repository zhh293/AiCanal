# ADR-0001：第一版实现基线

- 状态：已接受
- 日期：2026-08-13

## 决策

1. 源码暂时兼容 Java 11，CI 与生产目标 Java 21。原因是当前开发环境仅有 JDK 11；不得使用这一兼容性降低生产运行时基线。
2. Segmented WAL 默认段大小 1 GiB，GROUP_SYNC 窗口 5 ms、最多 256 条；磁盘 80% 暂停接入、70% 恢复，容量规划至少覆盖峰值接入 72 小时。
3. 第一版集群复制采用 Agent FANOUT。Agent 通过 Admin 拓扑 API 或静态列表发现所有接入节点，并分别等待 ACK；ZooKeeper 不承担数据复制。
4. TCP 允许多个 `consumerId`，各自使用 `tcp:{consumerId}` channel checkpoint。
5. 出口类型切换必须创建新 channel，并显式选择 `EARLIEST`、`LATEST` 或 `OFFSET`；不隐式复用旧出口 checkpoint。
6. 毒消息默认 `BLOCK`。只有带审计记录的人工授权才允许 `SKIP`。
7. namespace 格式为 `{environment}.{cluster}.{tenant}`，各段使用小写字母、数字和连字符。
8. 已发布配置变化通过快照 + 外部进程管理器整进程重启生效，不做实例热更新。

## 不变量

上述决策必须服从：不丢已持久化数据、无法证明 Leader 身份时停止供数、配置失败可恢复到 last-known-good。
