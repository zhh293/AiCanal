# 运维手册

## 启停与健康

服务由外部进程管理器启动。配置变更退出码为 `20`，应由 systemd、Kubernetes 或 Windows Service Wrapper 重新拉起。优雅停止窗口建议至少 90 秒。

- Liveness：JVM 主循环和 Netty EventLoop 存活。
- Readiness：配置有效、所有 enabled instance 已初始化、Netty 已监听。
- Destination health：分别检查实例状态、WAL、积压、Leader、出口和 checkpoint；不能把它们压缩为一个布尔值。

关键告警：WAL 磁盘使用率 >70%、接入暂停、CRC 错误、Leader session SUSPENDED/LOST、delivery lag 持续增长、死信 BLOCK、Admin 连续轮询失败。

## Raft 选主运维

- 使用 3 或 5 个奇数节点；所有节点配置相同的 `clusterId` 和有序 peer 集合，每个 `server.nodeId` 必须在 peer 集合中。
- Raft 端口是 UDP。仅允许配置内 peer 双向访问，禁止公网暴露；部署前确认 NAT 后的源 IP/端口仍与 peer 地址一致。
- `data/raft-election/{destination}/meta.properties` 是安全状态，和 WAL 一样放在节点独享的持久卷中。不得复制给另一个 nodeId，也不得为解决选举问题而随意删除。
- 失去多数派后 Leader 会在多数派租约到期前撤销本地 LeaderGuard。告警应同时观察“无 Leader”和“同一 destination 多 Leader”，后者必须立即隔离并停止下游提交。
- 当前成员集合是静态配置。扩缩容时先停止业务接入，统一修改所有节点配置，再整体启动并确认唯一 Leader；当前版本不支持在线成员变更。
- 上线前注入单节点宕机、双节点宕机、双向网络分区、30% UDP 丢包和时钟跳变，验证单 Leader、少数派不供数、恢复后 term 单调增加。

## WAL 备份

1. 将 destination 暂停接入并等待流水线清空，或使用存储级 crash-consistent snapshot。
2. 调用 flush/seal 后，复制 `data/{destination}/wal` 和 `data/config`。
3. 对备份生成 SHA-256 清单并在隔离位置验证可读。
4. 不单独备份 `.idx` 而遗漏 `.log`；日志是权威数据，索引可重建。

## 恢复演练

1. 在空数据目录恢复 WAL 和配置快照。
2. 使用相同或兼容的插件版本启动，但暂不接入生产流量。
3. 确认 CRC 扫描、尾部恢复、pending ingress 数、ready offset 和 checkpoint。
4. 校验非 Leader 不能供数，再开放 Agent FANOUT 和下游连接。
5. 每季度执行恢复演练，记录 RTO/RPO。ASYNC durability 的掉电窗口不属于零 RPO。

## 升级与回滚

- 协议和 WAL schema 必须向后兼容；先升级 Follower，再迁移 Leader。
- 发布前备份配置与 WAL，保留旧镜像和 last-known-good。
- 新版本在 startup grace period 内失败时移入 `rejected/`，恢复 last-known-good，抑制相同 hash 的重启循环。
- 不降级运行会写入旧版本无法识别 WAL 格式的版本。

## 容量基线

每个节点至少预留峰值接入 72 小时的数据空间。70% 告警，80% 停止新接入；绝不删除未确认事件自愈。WAL 段默认 1 GiB，清理只能越过所有有效 channel 的最小连续 checkpoint 并保留 safety window。
