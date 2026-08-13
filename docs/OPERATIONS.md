# 运维手册

## 启停与健康

服务由外部进程管理器启动。配置变更退出码为 `20`，应由 systemd、Kubernetes 或 Windows Service Wrapper 重新拉起。优雅停止窗口建议至少 90 秒。

- Liveness：JVM 主循环和 Netty EventLoop 存活。
- Readiness：配置有效、所有 enabled instance 已初始化、Netty 已监听。
- Destination health：分别检查实例状态、WAL、积压、Leader、出口和 checkpoint；不能把它们压缩为一个布尔值。

关键告警：WAL 磁盘使用率 >70%、接入暂停、CRC 错误、Leader session SUSPENDED/LOST、delivery lag 持续增长、死信 BLOCK、Admin 连续轮询失败。

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
