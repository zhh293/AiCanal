# DEVELOPMENT.md 实现状态

本清单按 `DEVELOPMENT.md` 的 25 个一级章节核对。它区分“仓库已实现并由自动测试覆盖”和“必须在真实外部基础设施执行的验收”，不把未运行的环境测试写成通过。

| 章节 | 状态 | 主要落点 |
| --- | --- | --- |
| 1–4 目标、名词、范围、架构 | 完成 | README、模块边界、控制面/数据面独立进程 |
| 5 工程模块 | 完成 | Maven 多模块与依赖分层、Java 11/21 CI |
| 6 领域模型 | 完成 | `canal-api` 不可变模型、校验、稳定哈希 |
| 7 CanalInstance | 完成 | 生命周期、每 destination Disruptor、暂停/恢复、恢复重放 |
| 8 Disruptor 流水线 | 完成 | parse → classify → dedup → audit → EVENT_READY，异常隔离 |
| 9 SPI | 完成 | 接口、注册表、ServiceLoader、重复/缺失 type 测试 |
| 10 WAL | 完成 | 分段二进制帧、CRC32C、SYNC/GROUP_SYNC、幂等索引、`.idx` 重建、checkpoint version+epoch CAS、尾部恢复。段清理遵循安全水位设计，当前由运维流程触发而非后台自动删除 |
| 11 EmbeddedController | 完成 | 状态、实例控制、检查、订阅、checkpoint 查询 |
| 12 Netty/TCP | 完成 | 帧编解码、角色、令牌鉴权、TLS/mTLS、PUBLISH/SUBSCRIBE/FETCH/ACK、EventLoop 外业务执行 |
| 13 三种 MQ | 完成 | 官方 Kafka/RocketMQ/RabbitMQ 客户端、confirm、重试、fencing、fsync 死信、BLOCK/SKIP |
| 14 ZooKeeper | 完成 | Curator 选主、epoch CAS、LeaderGuard、SUSPENDED/LOST fail-closed |
| 15 Admin | 完成 | JDBC 持久化版本、发布、回滚、diff、ETag、审计、RBAC 与 machine token |
| 16 配置轮询/重启 | 完成 | ETag 轮询、hash 校验、pending/active/LKG 原子快照、退出码 20、失败回退 |
| 17 配置示例 | 完成 | `canal-distribution/config/application.yaml` |
| 18 启动/运行流程 | 完成 | Server/Admin 可执行 fat JAR 与运行时装配 |
| 19 故障矩阵 | 完成 | CRC fail-closed、WAL-before-ACK、Leader fail-closed、重试/死信、配置回退 |
| 20 可观测性 | 基线完成 | live/ready/destinations、Prometheus 文本端点、结构化审计；生产可继续接入专用 metrics SDK |
| 21 安全 | 完成 | Admin RBAC、machine token、TCP 角色令牌、TLS/mTLS、Secret 禁止内联、非 root 部署 |
| 22 测试 | 自动化基线完成 | 单元、组件、真实 socket 端到端、恢复与不变量测试 |
| 23 开发阶段 | 完成 | 七阶段产物均已落库 |
| 24 风险边界 | 完成 | destination 隔离、单出口、WAL 权威、at-least-once、epoch fencing |
| 25 固化决策 | 完成 | `docs/adr/0001-baseline-decisions.md` |

## 需要真实基础设施的发布门禁

以下不是代码缺失，而是不能在当前无 Broker/多节点环境中诚实宣称已通过的上线验收：

- ZooKeeper 三节点分区、Leader 杀进程与接管演练。
- Kafka、RocketMQ、RabbitMQ 的 Broker 重启、confirm 丢失窗口和进程崩溃演练。
- 多 Canal Server 的 FANOUT、一主供数和滚动配置发布。
- 24/72 小时稳定性、磁盘高水位、数千 TCP 连接与容量基线。
- 用生产 CA 证书执行 TLS/mTLS 轮换和拒绝未授权客户端测试。

这些门禁的步骤与恢复要求见 `docs/OPERATIONS.md`，部署模板位于 `canal-distribution/`。
