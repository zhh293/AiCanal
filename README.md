# AI Canal

AI Canal 是一个以 `destination` 为隔离单元的资源接入、处理、可靠存储与分发系统。外部 Agent 主动将原始资源推送到 Canal Server；Server 在返回接入成功前先写入本地 WAL，再异步完成解析、分类、去重和审计，最终由当前 Leader 通过 TCP、Kafka、RocketMQ 或 RabbitMQ 向下游提供数据。

系统默认提供 **at-least-once** 语义：已经持久化的数据不会因为正常重启丢失，但在“下游已收到、checkpoint 尚未提交”的故障窗口内可能重复投递。下游应使用稳定的 `eventId` 做幂等处理。

> 生产运行推荐 Java 21；当前源码以 `--release 11` 编译，并在 Java 11 和 Java 21 上执行 CI 验证。

## 目录

- [系统解决什么问题](#系统解决什么问题)
- [架构总览](#架构总览)
- [核心数据流](#核心数据流)
- [可靠性与一致性语义](#可靠性与一致性语义)
- [工程模块](#工程模块)
- [技术栈](#技术栈)
- [快速开始](#快速开始)
- [Canal Server 配置](#canal-server-配置)
- [Admin 配置中心](#admin-配置中心)
- [TCP 数据协议](#tcp-数据协议)
- [健康检查与指标](#健康检查与指标)
- [数据目录](#数据目录)
- [构建、测试与编码规范](#构建测试与编码规范)
- [部署方式](#部署方式)
- [生产上线检查清单](#生产上线检查清单)
- [常见问题](#常见问题)
- [当前边界与后续工作](#当前边界与后续工作)
- [进一步阅读](#进一步阅读)

## 系统解决什么问题

AI Canal 适合下面这类场景：

1. 多个采集 Agent 需要主动推送网页、文档或其他资源。
2. 接入成功必须建立在明确的本地持久化边界上，不能只进入内存队列就返回 ACK。
3. 不同业务流需要按 `destination` 独立配置接入 ACL、处理插件、存储和出口。
4. 下游可能是长连接 TCP Consumer，也可能是 Kafka、RocketMQ 或 RabbitMQ。
5. 多节点部署时，同一 `destination` 只能有一个有效节点对外供数和提交 checkpoint。
6. 配置需要版本化、审计、发布、回滚，并在节点启动失败时恢复 last-known-good。

系统第一阶段不负责通用爬虫调度、跨地域数据复制、任意消息队列的 exactly-once，也不把 ZooKeeper 当作数据复制系统。

## 架构总览

AI Canal 分为数据面和控制面两个独立进程：

- **Canal Server**：接收 Agent 数据、写 WAL、运行处理流水线、选主并向下游供数。
- **Canal Admin**：保存不可变配置版本，负责校验、发布、回滚、审计和 Server 配置拉取。

```mermaid
flowchart LR
    subgraph Producers["资源生产端"]
        A1["Agent A"]
        A2["Agent B"]
    end

    subgraph DataPlane["Canal Server 数据面"]
        N["Netty TCP Server"]
        C["EmbeddedController"]
        I1["CanalInstance: destination-1"]
        I2["CanalInstance: destination-2"]
        W1["Segmented WAL"]
        W2["Segmented WAL"]
        D1["Disruptor Pipeline"]
        D2["Disruptor Pipeline"]
        L["LeaderGuard + epoch"]
        E["TCP / MQ Egress"]

        N --> C
        C --> I1
        C --> I2
        I1 --> W1
        I2 --> W2
        W1 --> D1
        W2 --> D2
        D1 --> E
        D2 --> E
        L --> E
    end

    subgraph ControlPlane["控制面"]
        Admin["Canal Admin"]
        DB["JDBC / H2"]
        ZK["ZooKeeper / Curator"]
        Admin --> DB
    end

    A1 -->|"HELLO + PUBLISH"| N
    A2 -->|"HELLO + PUBLISH"| N
    Admin -->|"ETag 轮询配置"| DataPlane
    ZK -->|"按 destination 选主与 fencing"| L
    E --> TCP["TCP Consumer"]
    E --> Kafka["Kafka"]
    E --> RocketMQ["RocketMQ"]
    E --> RabbitMQ["RabbitMQ"]
```

### destination 隔离

`destination` 是系统最重要的隔离边界。每个启用的 destination 拥有独立的：

- 接入 ACL 和批量大小限制；
- Parser、Classifier、Deduplicator、AuditLogger 插件实例；
- Disruptor 处理流水线；
- WAL 目录、事件索引和 checkpoint；
- Leader 身份与 fencing epoch；
- 唯一出口类型和出口 Worker。

一个 destination 同时只能选择一个出口：`TCP`、`KAFKA`、`ROCKETMQ` 或 `RABBITMQ`。

### 多节点模型

ZooKeeper 只负责协调和 fencing，不复制 WAL 数据。当前集群复制基线是 **Agent FANOUT**：Agent 将同一个 `requestId` 和相同内容推送到所有目标节点，每个节点分别写自己的本地 WAL；Leader 只负责下游供数。

因此，多节点生产部署必须同时满足：

- Agent 知道所有需要接收该 destination 的节点；
- Agent 对每个副本使用相同 `requestId` 和内容；
- 每个节点使用独立、持久化的数据卷；
- ZooKeeper 不可用或连接进入 `SUSPENDED`、`LOST`、`READ_ONLY` 时，节点立即停止供数和 checkpoint 提交。

## 核心数据流

### Agent 接入流程

```mermaid
sequenceDiagram
    participant Agent
    participant Netty as "Netty Server"
    participant Instance as "CanalInstance"
    participant WAL as "Segmented WAL"
    participant Pipeline as "Disruptor Pipeline"

    Agent->>Netty: "HELLO(role=AGENT_PRODUCER)"
    Netty-->>Agent: "HELLO_ACK"
    Agent->>Netty: "PUBLISH(requestId, destination, records, checksum)"
    Netty->>Instance: "校验角色、Agent ACL、大小和 checksum"
    Instance->>WAL: "追加 INGEST_RAW"
    WAL->>WAL: "按当前 GROUP_SYNC 策略刷盘"
    WAL-->>Instance: "ingestSequence"
    Instance-->>Agent: "PUBLISH_ACK"
    Instance->>Pipeline: "异步发布 WAL pointer"
    Pipeline->>Pipeline: "parse → classify → dedup → audit"
    Pipeline->>WAL: "追加 EVENT_READY 或 PROCESS_REJECTED"
```

`PUBLISH_ACK` 的含义是整个 `INGEST_RAW` 已达到当前运行时的 `GROUP_SYNC` durability 边界，而不是数据已经投递到下游。`storage.config.durability` 目前尚未接入接入链路，修改样例字段不会改变这一行为。

### TCP 下游流程

1. Consumer 以 `DATA_CONSUMER` 角色完成 HELLO。
2. Consumer 订阅一个 TCP 类型的 destination。
3. Server 验证本节点仍是该 destination 的 Leader，返回 `channelId` 和 `epoch`。
4. Consumer 使用 FETCH 拉取下一批 `EVENT_READY`。
5. Consumer 处理完成后提交连续 `ackOffset` 和订阅时获得的 `epoch`。
6. Server 通过 checkpoint version + epoch CAS 提交进度，再返回 `ACK_COMMITTED`。
7. 连接中断但 ACK 未提交时，下次会从已提交 checkpoint 后重新投递。

每个 TCP `consumerId` 使用独立的 `tcp:{consumerId}` checkpoint channel。

### MQ 下游流程

1. 每个 MQ destination 创建一个逻辑 Worker。
2. Worker 只有在本节点为对应 destination Leader 时才读取事件。
3. Worker 从 channel checkpoint 后批量读取 `EVENT_READY`。
4. Producer 等待 Broker ACK 或 publisher confirm。
5. 发送成功后再次检查 Leader epoch。
6. 通过 version + epoch CAS 提交 checkpoint。
7. 失败时执行指数退避；达到上限后先 fsync 到 `dead-letter/*.jsonl`。
8. 默认 `BLOCK` 停住后续投递；显式配置 `SKIP` 时才推进 checkpoint。

## 可靠性与一致性语义

系统坚持以下不变量：

- **WAL-before-ACK**：`INGEST_RAW` 达到当前 `GROUP_SYNC` durability 边界后才能返回接入 ACK。
- **存储与流水线解耦**：TCP/MQ 只读取 `EVENT_READY`，不直接消费 Disruptor。
- **幂等接入**：相同 Agent、destination、requestId 和内容返回相同 ACK，不重复追加；相同 requestId 但不同 checksum 返回 `REQUEST_ID_CONFLICT`。
- **稳定事件标识**：相同 destination、sourceKey 和规范化内容产生相同 `eventId`。
- **at-least-once**：发送成功但 checkpoint 提交前崩溃时允许重发，不静默丢失。
- **checkpoint 单调推进**：不能倒退、不能越过已发送水位，并通过期望 version 做 CAS。
- **epoch fencing**：非 Leader 或旧 epoch 不能读取生产数据和提交 checkpoint。
- **损坏 fail-closed**：活动段尾部半写会截断到最后完整记录；历史记录 CRC 损坏会拒绝启动，不静默忽略。
- **索引可重建**：`.log` 是权威数据，`.idx` 损坏时从 WAL 扫描重建。

WAL 记录主要包括：

| 类型 | 含义 |
| --- | --- |
| `INGEST_RAW` | 已可靠接收的 Agent 原始批次 |
| `EVENT_READY` | 已完成处理、可以向下游提供的事件 |
| `PROCESS_REJECTED` | 某条原始记录已经被确定拒绝或去重 |
| `DELIVERY_COMMIT` | 某个出口 channel 的连续提交进度 |
| `SEGMENT_SEAL` | 格式中已识别的预留段密封类型；当前滚段流程不主动写入 |

## 工程模块

| 模块 | 职责 |
| --- | --- |
| `canal-api` | 领域模型、错误模型、稳定哈希和批次 checksum |
| `canal-spi` | 插件契约、生命周期、PluginContext 和 PluginRegistry |
| `canal-core` | CanalInstance、Disruptor 流水线及默认处理插件 |
| `canal-storage-api` | EventStore、恢复计划、StoredEvent 和 checkpoint SPI |
| `canal-storage-default` | 分段二进制 WAL、CRC32C、稀疏索引、恢复与 group fsync |
| `canal-cluster-api` | LeaderElector、LeaderGuard、Leadership 和 fencing 契约 |
| `canal-cluster-zookeeper` | 基于 Curator/LeaderLatch 的 ZooKeeper 选主和 epoch CAS |
| `canal-ingress-netty` | TCP 帧模型、编解码器和默认 AgentDataReceiver |
| `canal-egress-api` | MQ Producer SPI、通用 Worker、重试和持久化死信 |
| `canal-egress-netty` | TCP Subscription、FETCH 和连续 ACK 语义 |
| `canal-egress-kafka` | Kafka 官方 Producer 适配器 |
| `canal-egress-rocketmq` | RocketMQ 官方 Producer 适配器 |
| `canal-egress-rabbitmq` | RabbitMQ persistent + mandatory + confirm 适配器 |
| `canal-admin-api` | Admin 配置版本和运行时响应模型 |
| `canal-admin-server` | Admin HTTP API、RBAC、JDBC Repository、发布与回滚 |
| `canal-server` | 生产运行时装配、TCP Server、健康检查、配置轮询和入口类 |
| `canal-testkit` | 等待与记录型测试辅助组件 |
| `canal-distribution` | 默认配置、Docker、Kubernetes、systemd 和 Windows Service 资产 |

模块依赖大致如下：

```text
canal-api
├── canal-spi
├── canal-storage-api ── canal-storage-default
├── canal-cluster-api ── canal-cluster-zookeeper
├── canal-egress-api
│   ├── canal-egress-netty
│   ├── canal-egress-kafka
│   ├── canal-egress-rocketmq
│   └── canal-egress-rabbitmq
├── canal-ingress-netty
├── canal-core
└── canal-server

canal-admin-api ── canal-admin-server
```

## 技术栈

版本以父 POM 为准：

| 领域 | 技术 | 当前版本/说明 |
| --- | --- | --- |
| 语言与运行时 | Java | 源码 release 11；生产推荐 JDK/JRE 21 |
| 构建 | Maven | 最低 3.8；工作区使用 3.9.9 |
| TCP 网络 | Netty | 4.1.116.Final |
| 内存流水线 | LMAX Disruptor | 3.4.4 |
| 序列化/配置 | Jackson Databind + YAML | 2.18.2 |
| 集群协调 | Apache Curator | 5.7.1 |
| Kafka | `kafka-clients` | 3.9.0 |
| RocketMQ | `rocketmq-client` | 4.9.8 |
| RabbitMQ | Java AMQP Client | 5.25.0 |
| Admin 数据库 | JDBC + H2 | H2 2.3.232，默认文件数据库 |
| 测试 | JUnit Jupiter | 5.11.4 |
| Java 格式化 | Spotless + Google Java Format | 2.44.5 / 1.22.0 |
| POM 格式化 | SortPom | 3.4.1 |
| 容器运行时 | Eclipse Temurin | Docker 镜像使用 Java 21 JRE |

## 快速开始

### 环境要求

- JDK 11 或更高版本；生产建议 JDK 21。
- Maven 3.8 或更高版本，或者仓库内已经准备好的 Maven Wrapper 运行时。
- 本地默认启动不需要 ZooKeeper 和 MQ，默认配置使用 `standalone + TCP`。
- TCP 默认端口 `11111`，健康检查默认端口 `11112`；启动前确认没有被占用。

查看版本：

```bash
java -version
./mvnw -version
```

Windows PowerShell：

```powershell
java -version
.\mvnw.cmd -version
```

Wrapper 不会在运行时自动下载 Maven 可执行文件。如果 `.mvn/wrapper/apache-maven-3.9.9/` 不存在，请安装 Maven 并将下面命令中的 `./mvnw` 替换为 `mvn`。

### 1. 完整验证

Linux/macOS：

```bash
./mvnw clean verify
```

Windows PowerShell：

```powershell
.\mvnw.cmd clean verify
```

`verify` 会执行编译、全部自动测试、Spotless Java 格式检查和 SortPom POM 检查。

### 2. 构建可执行 JAR

```bash
./mvnw -pl canal-server,canal-admin-server -am package
```

Windows：

```powershell
.\mvnw.cmd -pl canal-server,canal-admin-server -am package
```

主要产物：

```text
canal-server/target/canal-server-1.0.0-SNAPSHOT-all.jar
canal-admin-server/target/canal-admin-server-1.0.0-SNAPSHOT-all.jar
```

带 `-all.jar` 后缀的是包含运行时依赖的可执行胖 JAR。

### 3. 启动 standalone Server

默认配置位于 `canal-distribution/config/application.yaml`，包含一个 TCP 出口的 `example-resources` destination。

```bash
java -jar canal-server/target/canal-server-1.0.0-SNAPSHOT-all.jar \
  --config canal-distribution/config/application.yaml
```

Windows PowerShell：

```powershell
java -jar canal-server/target/canal-server-1.0.0-SNAPSHOT-all.jar `
  --config canal-distribution/config/application.yaml
```

控制台出现下面的文本表示运行时装配和监听完成：

```text
AI Canal READY
```

### 4. 检查服务

```bash
curl -i http://127.0.0.1:11112/health/live
curl -i http://127.0.0.1:11112/health/ready
curl -i http://127.0.0.1:11112/health/destinations
curl -i http://127.0.0.1:11112/metrics
```

PowerShell：

```powershell
Invoke-RestMethod http://127.0.0.1:11112/health/live
Invoke-RestMethod http://127.0.0.1:11112/health/ready
Invoke-RestMethod http://127.0.0.1:11112/health/destinations
Invoke-WebRequest http://127.0.0.1:11112/metrics
```

预期 readiness 类似：

```json
{"status":"READY","uptimeMillis":"1234","instances":"1"}
```

## Canal Server 配置

### 顶层结构

```yaml
namespace: local.main.default
version: local-1

cluster:
  mode: standalone

server:
  nodeId: ${HOSTNAME}
  dataDir: ./data
  health:
    port: 11112
  netty:
    port: 11111
    authentication:
      required: false

destinations:
  - id: example-resources
    enabled: true
    ingress: {}
    receiver: {}
    parser: {}
    classifier: {}
    deduplicator: {}
    logger: {}
    storage: {}
    egress: {}
```

关键顶层字段：

| 字段 | 必填 | 含义 |
| --- | --- | --- |
| `namespace` | 建议 | 配置命名空间，Admin 要求格式为 `environment.cluster.tenant` |
| `version` | 建议 | 当前配置版本标识，写入事件元数据 |
| `cluster.mode` | 否 | `standalone` 或 `zookeeper`，默认 `standalone` |
| `server.nodeId` | 否 | 节点 ID；精确值 `${HOSTNAME}` 时从环境变量展开 |
| `server.dataDir` | 否 | WAL、checkpoint、配置快照和死信根目录，默认 `./data` |
| `server.netty.port` | 否 | TCP 数据端口，默认 `11111` |
| `server.health.port` | 否 | HTTP 健康端口；`0` 表示不启动，样例为 `11112` |
| `destinations` | 是 | destination 数组，ID 必须唯一 |

### destination 配置

```yaml
destinations:
  - id: example-resources
    enabled: true
    ingress:
      mode: FANOUT
      allowedAgents: [example-agent]
      maxBatchRecords: 500
      maxBatchBytes: 4194304
      maxRecordBytes: 1048576
    receiver:
      type: netty-default
      config:
        requireBatchChecksum: true
    parser:
      type: html-default
      config: {}
    classifier:
      type: rule-default
      config:
        rules:
          - category: ai
            keywords: [AI, LLM, Agent]
    deduplicator:
      type: hash-default
      config: {}
    logger:
      type: slf4j-json
      config: {}
    storage:
      type: segmented-wal
      config:
        durability: GROUP_SYNC
    egress:
      type: TCP
      channelId: tcp:default
      tcp: {}
```

接入限制由 Server 在写 WAL 前检查：

- `allowedAgents` 必须包含 HELLO/PUBLISH 使用的 `agentId`；空数组表示不允许任何 Agent，不是通配；
- 单批记录数不能超过 `maxBatchRecords`；
- 单条 payload 不能超过 `maxRecordBytes`；
- 批次 payload 总和不能超过 `maxBatchBytes`；
- 同一批次只能属于一个 destination。

默认 SPI type：

| 组件 | type |
| --- | --- |
| Agent receiver | `netty-default` |
| Parser | `html-default` |
| Classifier | `rule-default` |
| Deduplicator | `hash-default` |
| Audit logger | `slf4j-json` |
| Storage | `segmented-wal` |
| Standalone leader elector | `standalone` |
| ZooKeeper leader elector | `zookeeper` |

### ZooKeeper 集群模式

```yaml
cluster:
  mode: zookeeper
  zookeeper:
    connectString: zk-1:2181,zk-2:2181,zk-3:2181
    namespace: ai-canal
```

每个 destination 在下面的路径参与选主并维护 epoch：

```text
/{namespace}/destinations/{destination}/candidates
/{namespace}/destinations/{destination}/epoch
```

ZooKeeper 会话状态进入 `SUSPENDED`、`LOST` 或 `READ_ONLY` 时，本地 LeaderGuard 立即撤销领导权。再次获得领导权时通过 CAS 增加 epoch。

### TCP 角色鉴权

开发配置可以关闭角色令牌；生产建议强制开启：

```bash
export CANAL_TCP_REQUIRE_AUTH=true
export CANAL_AGENT_TOKEN='replace-with-agent-secret'
export CANAL_CONSUMER_TOKEN='replace-with-consumer-secret'
export CANAL_MONITOR_TOKEN='replace-with-monitor-secret'
```

PowerShell：

```powershell
$env:CANAL_TCP_REQUIRE_AUTH = "true"
$env:CANAL_AGENT_TOKEN = "replace-with-agent-secret"
$env:CANAL_CONSUMER_TOKEN = "replace-with-consumer-secret"
$env:CANAL_MONITOR_TOKEN = "replace-with-monitor-secret"
```

开启鉴权时三个角色令牌必须全部存在。HELLO 中的 `token` 只与所声明角色对应的令牌比较，不会写入日志。

### TLS 与 mTLS

启用服务端 TLS：

```bash
export CANAL_TLS_CERT=/run/secrets/canal-server.crt
export CANAL_TLS_KEY=/run/secrets/canal-server.pkcs8.key
```

要求客户端证书：

```bash
export CANAL_TLS_TRUST_CERT=/run/secrets/client-ca.crt
export CANAL_TLS_REQUIRE_CLIENT_AUTH=true
```

约束：

- `CANAL_TLS_CERT` 和 `CANAL_TLS_KEY` 必须同时配置；
- mTLS 还必须提供 `CANAL_TLS_TRUST_CERT`；
- 支持 TLS 1.3 和 TLS 1.2；
- 私钥和 token 只通过 Secret 挂载或环境注入，不要提交到仓库。

### MQ 出口配置

公共 Worker 参数放在当前 MQ 类型的配置块中：

```yaml
batchSize: 200
maxBatchBytes: 1048576
maxAttempts: 8
initialBackoffMillis: 1000
maxBackoffMillis: 300000
deadLetterPolicy: BLOCK
```

Kafka：

```yaml
egress:
  type: KAFKA
  channelId: kafka:resource-events
  kafka:
    bootstrapServers: kafka-1:9092,kafka-2:9092
    topic: resource-events
    acks: all
    enableIdempotence: true
    compression: zstd
    lingerMs: 5
    batchSize: 200
    maxBatchBytes: 1048576
    maxAttempts: 8
    deadLetterPolicy: BLOCK
```

Kafka 使用 `eventId` 作为 message key 和 header，默认 `acks=all` 并启用幂等 producer。

RocketMQ：

```yaml
egress:
  type: ROCKETMQ
  channelId: rocketmq:resource-events
  rocketmq:
    nameServer: rmq-ns-1:9876;rmq-ns-2:9876
    topic: resource-events
    sendTimeoutMillis: 10000
    batchSize: 200
    maxAttempts: 8
    deadLetterPolicy: BLOCK
```

RocketMQ 同步发送；`eventId` 写入 message key，事件分类写入 tag。

RabbitMQ：

```yaml
egress:
  type: RABBITMQ
  channelId: rabbitmq:resource-events
  rabbitmq:
    uri: amqps://user:password@rabbitmq.example.com/vhost
    exchange: resource.events
    routingKey: resource.ready
    batchSize: 200
    maxAttempts: 8
    deadLetterPolicy: BLOCK
```

RabbitMQ 消息为 persistent，发送时启用 `mandatory` 并等待 publisher confirm；Broker 上的 exchange、queue 和 binding 需要预先创建。

> 不要将包含密码的 RabbitMQ URI提交到 Git。生产配置应由 Secret Manager 在启动前解析或通过受控文件生成。

### 当前固定运行时参数

配置样例中保留了一些面向后续版本的调优字段。当前实现的实际值如下：

| 项目 | 当前实际行为 |
| --- | --- |
| 接入 `INGEST_RAW` durability | 固定为 `GROUP_SYNC` |
| `EVENT_READY` / `PROCESS_REJECTED` durability | 固定为 `GROUP_SYNC` |
| `DELIVERY_COMMIT` durability | 固定为 `SYNC` |
| Disruptor ring size | 每 destination 固定为 1024 |
| WAL segment max size | 运行时固定为 1 GiB |
| WAL group sync | 固定 5 ms 或 256 条触发 |
| Netty 最大帧 | 固定 8 MiB |
| Netty business executor | `max(2, availableProcessors)` |
| TCP FETCH 默认值 | `limit=100`、`maxBytes=1 MiB`，客户端可在请求中覆盖 |

`server.disruptor.*`、部分 `server.netty.*`、`storage.config.*` 和 `egress.tcp.*` 字段目前主要用于配置契约和未来调优，尚未全部接入运行时。不要仅修改这些字段就假设行为已经变化。

## Admin 配置中心

Admin 是独立 Java 进程，默认监听 `8080`，默认使用 H2 文件数据库：

```text
jdbc:h2:file:./data/admin/ai-canal
```

### Admin 环境变量

| 变量 | 必填 | 默认值/用途 |
| --- | --- | --- |
| `CANAL_ADMIN_PORT` | 否 | `8080` |
| `CANAL_ADMIN_JDBC_URL` | 否 | `jdbc:h2:file:./data/admin/ai-canal` |
| `CANAL_ADMIN_DB_USER` | 否 | `sa` |
| `CANAL_ADMIN_DB_PASSWORD` | 否 | 空字符串，仅适合本地开发 |
| `CANAL_ADMIN_VIEWER_TOKEN` | 条件 | Viewer bearer token |
| `CANAL_ADMIN_EDITOR_TOKEN` | 条件 | Editor bearer token |
| `CANAL_ADMIN_PUBLISHER_TOKEN` | 条件 | Publisher bearer token |
| `CANAL_ADMIN_ADMIN_TOKEN` | 条件 | Admin bearer token，可访问所有管理路由 |
| `CANAL_ADMIN_MACHINE_TOKEN` | 是 | Canal Server 拉取已发布配置的机器凭证 |

至少配置一个管理端 bearer token，并且必须配置 machine token。样例见 `canal-distribution/admin.env.example`；其中 `${secret:...}` 是部署平台应解析的占位符，不能原样当作生产密钥。

Admin 当前使用 JDK `HttpServer`，不内置 HTTPS。生产环境应只监听受控网络，并通过支持 TLS/mTLS 的反向代理或 Service Mesh 暴露；不要把 8080 直接开放到公网。

### 启动 Admin

Linux/macOS：

```bash
export CANAL_ADMIN_ADMIN_TOKEN='local-admin-token'
export CANAL_ADMIN_MACHINE_TOKEN='local-machine-token'
export CANAL_ADMIN_JDBC_URL='jdbc:h2:file:./data/admin/ai-canal'

java -jar canal-admin-server/target/canal-admin-server-1.0.0-SNAPSHOT-all.jar
```

PowerShell：

```powershell
$env:CANAL_ADMIN_ADMIN_TOKEN = "local-admin-token"
$env:CANAL_ADMIN_MACHINE_TOKEN = "local-machine-token"
$env:CANAL_ADMIN_JDBC_URL = "jdbc:h2:file:./data/admin/ai-canal"

java -jar canal-admin-server/target/canal-admin-server-1.0.0-SNAPSHOT-all.jar
```

启动成功：

```text
AI Canal Admin READY port=8080
```

浏览器打开 `http://127.0.0.1:8080/` 即可进入内置管理控制台。控制台静态资源直接打包在 Admin JAR 中，不依赖 Node.js、CDN 或额外 Web Server，适合内网和离线部署。

首次进入时输入一个管理 bearer token 和操作者名称。token 仅保存在当前浏览器的 `sessionStorage`；关闭标签页后会话凭据消失。控制台会根据 token 对应角色自动隐藏无权限操作：

- **控制总览**：查看当前活动版本、release 数量、destination 数量、内容指纹和最近发布；
- **发布档案**：筛选和搜索不可变版本，查看内容、比较 diff、发布或回滚；
- **配置工坊**：编辑 YAML、查看行号与字节数、运行服务端验证并创建 release；
- **审计轨迹**：查看 release、publish 和 rollback 的操作者、命名空间、版本与时间；
- **响应式导航**：桌面使用固定侧栏，平板和手机使用带遮罩的抽屉导航。

生产环境仍应通过 HTTPS/mTLS 反向代理访问控制台。浏览器 token 代表对应管理权限，不要在共享终端上使用长期有效的高权限 token。

健康检查：

```bash
curl http://127.0.0.1:8080/health/live
```

### RBAC

| 角色 | 能力 |
| --- | --- |
| `VIEWER` | 查看 release 列表和 diff |
| `EDITOR` | 查看、校验和创建不可变 release |
| `PUBLISHER` | 查看、发布和回滚 |
| `ADMIN` | 所有管理能力 |
| Machine credential | 仅供 Server 拉取当前已发布配置 |

管理请求使用：

```http
Authorization: Bearer <management-token>
X-Actor: alice
```

`X-Actor` 可选，用于审计；未提供时使用角色名。

### Admin API

| 方法与路径 | 权限 | 说明 |
| --- | --- | --- |
| `GET /` | 无 | 内置 Admin 管理控制台 |
| `GET /health/live` | 无 | Admin liveness |
| `GET /api/v1/session` | Viewer+ | 返回当前管理凭据的角色和操作者 |
| `GET /api/v1/namespaces` | Viewer+ | 返回所有已创建命名空间 |
| `GET /api/v1/namespaces/{ns}/releases` | Viewer+ | 查看 release 列表 |
| `POST /api/v1/namespaces/{ns}/validate` | Editor/Admin | 校验 YAML/JSON 内容 |
| `POST /api/v1/namespaces/{ns}/releases` | Editor/Admin | 创建不可变 release |
| `POST /api/v1/namespaces/{ns}/releases/{version}/publish` | Publisher/Admin | 发布指定版本 |
| `POST /api/v1/namespaces/{ns}/releases/{version}/rollback` | Publisher/Admin | 基于旧版本创建新的已发布版本 |
| `GET /api/v1/namespaces/{ns}/releases/{from}/diff/{to}` | Viewer+ | 查看两个版本的文本差异 |
| `GET /api/v1/audit` | Viewer+ | 返回配置控制审计记录 |
| `GET /api/v1/runtime-config/{ns}` | Machine token | Server 拉取当前已发布配置，支持 ETag/304 |

创建 release 的请求体：

```json
{
  "content": "namespace: production.main.default\ndestinations: []\n",
  "comment": "initial release"
}
```

示例：

```bash
curl -X POST http://127.0.0.1:8080/api/v1/namespaces/production.main.default/releases \
  -H 'Authorization: Bearer local-admin-token' \
  -H 'X-Actor: alice' \
  -H 'Content-Type: application/json' \
  -d '{"content":"namespace: production.main.default\ndestinations: []\n","comment":"initial release"}'

curl -X POST \
  http://127.0.0.1:8080/api/v1/namespaces/production.main.default/releases/1/publish \
  -H 'Authorization: Bearer local-admin-token' \
  -H 'X-Actor: alice'

curl http://127.0.0.1:8080/api/v1/runtime-config/production.main.default \
  -H 'Authorization: Bearer local-machine-token'
```

Admin 发布前会检查 namespace、destination 唯一性、出口类型、基本接入限制以及明文 secret/password/token。发布内容按 hash 去重，已创建的 release 不会被原地修改。

### Server 轮询 Admin

为 Canal Server 增加：

```bash
export CANAL_ADMIN_BASE_URL=http://127.0.0.1:8080
export CANAL_ADMIN_MACHINE_TOKEN='local-machine-token'
export CANAL_ADMIN_POLL_SECONDS=30
```

轮询流程：

1. Server 请求 `/api/v1/runtime-config/{namespace}`，携带 machine bearer token。
2. 后续请求携带 `If-None-Match`；未变化时 Admin 返回 `304`。
3. 新版本返回 `200` 后，Server 校验内容和 SHA-256 hash。
4. 配置原子写入 `data/config/pending.json` 和元数据。
5. Server 进入外部重启流程并以退出码 `20` 退出。
6. 新进程优先加载 pending；启动成功后提升为 active。
7. 启动失败时将 pending 移入 `rejected/`，恢复 active 或 last-known-good。

进程管理器必须把退出码 `20` 视为“需要重新拉起”，但不应对正常退出码 `0` 无限重启。

## TCP 数据协议

TCP 帧使用大端序：

```text
+----------+---------+---------+----------+-----------+------------+
| magic(2) | ver(1)  | type(1) | flags(2) | length(4) | payload(N) |
+----------+---------+---------+----------+-----------+------------+
```

- magic：`0xA1CA`
- protocol version：`1`
- payload：UTF-8 JSON
- 最大帧：8 MiB
- 非法 magic、version 或 length 会关闭连接
- WAL、读取和 checkpoint 等阻塞操作在 Netty EventLoop 之外执行

连接必须首先发送 HELLO：

```json
{
  "clientId": "example-agent",
  "role": "AGENT_PRODUCER",
  "token": "agent-role-token"
}
```

角色：

| 角色 | 允许命令 |
| --- | --- |
| `AGENT_PRODUCER` | PING、PUBLISH、STATUS |
| `DATA_CONSUMER` | PING、SUBSCRIBE、FETCH、ACK、STATUS |
| `MONITOR` | PING、STATUS |

主要消息类型：

| 名称 | ID | 方向 |
| --- | ---: | --- |
| `HELLO` / `HELLO_ACK` | 1 / 2 | Client ↔ Server |
| `PING` / `PONG` | 3 / 4 | Client ↔ Server |
| `PUBLISH` / `PUBLISH_ACK` / `PUBLISH_NACK` | 5 / 6 / 7 | Agent ↔ Server |
| `SUBSCRIBE` / `SUBSCRIBE_ACK` | 8 / 9 | Consumer ↔ Server |
| `FETCH` / `DATA_BATCH` | 10 / 11 | Consumer ↔ Server |
| `ACK` / `ACK_COMMITTED` | 12 / 13 | Consumer ↔ Server |
| `STATUS` / `STATUS_ACK` | 20 / 21 | Client ↔ Server |
| `ERROR` | 127 | Server → Client |

PUBLISH 需要 `agentId`、`requestId`、`destination`、`protocolVersion`、`sentAt`、`records` 和 `batchChecksum`。checksum 使用 `BatchChecksums.sha256` 的确定性算法，覆盖 Agent、request、destination，以及排序后的 header 和所有 payload。

协议细节及 ACK 语义见 `docs/PROTOCOL.md`，可运行的 socket 交互示例见 `canal-server/src/test/java/io/github/aicanal/server/NettyDataPlaneTest.java`。

## 健康检查与指标

Canal Server 健康端点：

| 路径 | 成功码 | 含义 |
| --- | ---: | --- |
| `/health/live` | 200 | HTTP 健康服务存活 |
| `/health/ready` | 200 或 503 | 所有已创建 instance 是否处于 RUNNING |
| `/health/destinations` | 200 | destination 到实例状态的映射 |
| `/metrics` | 200 | Prometheus 文本格式的基础指标 |

当前基础指标：

```text
ai_canal_up
ai_canal_ready
ai_canal_instances
ai_canal_uptime_milliseconds
```

Admin 当前提供 `/health/live`。Admin 暂时不可用不会使已经启动的数据节点停止工作；节点继续使用 active 配置，并在下一轮轮询重试。

Canal Server 的健康与指标端点当前不要求认证，并监听配置端口的所有网络接口。生产环境应使用防火墙、Security Group 或 Kubernetes NetworkPolicy 限制访问范围。

## 数据目录

以 `server.dataDir=./data` 为例：

```text
data/
├── {destination}/
│   ├── wal/
│   │   ├── 00000000000000000001.log
│   │   └── 00000000000000000001.idx
│   └── dead-letter/
│       └── {channel}.jsonl
├── config/
│   ├── active.json
│   ├── active.meta
│   ├── pending.json
│   ├── pending.meta
│   ├── last-known-good.json
│   ├── last-known-good.meta
│   └── rejected/
└── admin/
    └── ai-canal.mv.db
```

注意：

- `.log` 是 WAL 权威数据，不能只备份 `.idx`；
- `.idx` 可从 `.log` 重建；
- `dead-letter` 在 MQ 失败达到重试上限时先 fsync，再决定 BLOCK 或 SKIP；
- `data/config` 决定配置重启和回滚能力；
- 多节点不能共享同一个可写本地 WAL 目录；每个节点应有独立数据卷。

## 构建、测试与编码规范

### 常用 Maven 命令

```bash
# 完整验证
./mvnw clean verify

# 只测试 Server 及其依赖
./mvnw -pl canal-server -am test

# 构建两个可执行进程
./mvnw -pl canal-server,canal-admin-server -am package

# 自动格式化 Java 和 POM
./mvnw spotless:apply sortpom:sort -Dsort.createBackupFile=false
```

Windows 将 `./mvnw` 替换为 `.\mvnw.cmd`。

### 格式门禁

- Java 使用 Google Java Format，2 空格缩进；
- POM 使用 SortPom；
- `.editorconfig` 统一 UTF-8、LF 和文件末尾换行；
- `mvn verify` 会检查 Java 和 POM 格式，不合规则构建失败；
- 详细规则见 `docs/CODING_STYLE.md`。

### 测试覆盖重点

- eventId 确定性；
- SPI type 重复和缺失；
- requestId 幂等和 checksum 冲突；
- WAL 尾部半写恢复、CRC fail-closed 和索引重建；
- 多记录批次的部分恢复；
- checkpoint version + epoch CAS；
- LeaderGuard fail-closed；
- TCP 越界 ACK、旧 epoch 和真实 socket 端到端；
- Config release、发布、回滚、RBAC 和 JDBC 重启持久化；
- pending/active/last-known-good 配置回退；
- 持久化 MQ 死信。

CI 在 Java 11 和 Java 21 上运行 `mvn verify`，定义见 `.github/workflows/ci.yml`。

## 部署方式

### Docker

先构建 Server 胖 JAR：

```bash
./mvnw -pl canal-server -am package
docker build -f canal-distribution/Dockerfile -t ai-canal:1.0.0 .
docker run --rm \
  -p 11111:11111 \
  -p 11112:11112 \
  -v canal-data:/opt/ai-canal/data \
  ai-canal:1.0.0
```

镜像特性：

- 基于 `eclipse-temurin:21-jre`；
- UID 10001 非 root 运行；
- JVM 使用 `-XX:MaxRAMPercentage=75`；
- 数据目录为 `/opt/ai-canal/data`；
- 暴露 11111 和 11112。

### Docker Compose

```bash
./mvnw -pl canal-server -am package
docker compose -f canal-distribution/docker-compose.yaml up --build
```

Compose 使用只读根文件系统、独立 named volume 和 `/tmp` tmpfs。生产环境还需要通过 secrets 注入鉴权令牌和 TLS 证书。

### Kubernetes

模板位于 `canal-distribution/k8s/canal-server.yaml`：

```bash
docker build -f canal-distribution/Dockerfile -t registry.example.com/ai-canal:1.0.0 .
docker push registry.example.com/ai-canal:1.0.0

# 先将模板中的 image 修改为实际镜像
kubectl apply -f canal-distribution/k8s/canal-server.yaml
kubectl rollout status statefulset/ai-canal
```

模板使用 StatefulSet、每 Pod 独立 PVC、非 root securityContext、90 秒终止窗口和 HTTP liveness/readiness probe。生产应用前必须补充：

> **重要：** 模板默认是 `replicas: 3`，镜像内默认配置却是 `cluster.mode: standalone`。不能原样作为三节点生产集群部署，否则每个 Pod 都会认为自己是 Leader。验证单机时先将副本数改为 1；运行多副本前必须挂载 ZooKeeper 集群配置，并按 Agent FANOUT 模型将相同批次送达所有候选节点。

- ConfigMap 或只读配置卷；
- Secret 形式的角色令牌、Admin machine token 和 TLS 文件；
- ZooKeeper 地址；
- PodDisruptionBudget、反亲和和拓扑分布约束；
- 适合业务峰值的 CPU、内存和 PVC 容量；
- NetworkPolicy 和 TLS 入口策略。

### systemd

模板位于 `canal-distribution/systemd/ai-canal.service`。典型目录：

```text
/opt/ai-canal/app.jar
/etc/ai-canal/application.yaml
/var/lib/ai-canal/
```

安装后：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now ai-canal
sudo systemctl status ai-canal
sudo journalctl -u ai-canal -f
```

部署前检查 service 的 `WorkingDirectory`、配置路径和 `server.dataDir` 是否一致。当前仓库模板把退出码 `20` 列为 `SuccessExitStatus`，因此在 `Restart=on-failure` 下不会因配置变更自动重启；如果要启用 Admin 热发布流程，需要在受控部署版本的 unit 中增加 `RestartForceExitStatus=20`，然后执行 `systemctl daemon-reload`。正常退出码 `0` 不应进入无限重启循环。

### Windows Service

`canal-distribution/windows/ai-canal.xml` 适用于 WinSW。将下面文件放在同一服务目录：

```text
ai-canal.exe        # WinSW 可执行文件
ai-canal.xml
app.jar
config/application.yaml
```

然后以管理员 PowerShell 执行：

```powershell
.\ai-canal.exe install
.\ai-canal.exe start
.\ai-canal.exe status
```

模板配置了 90 秒停止超时和日志滚动。凭证应通过系统环境、受限 ACL 文件或 Secret 管理器提供。

## 生产上线检查清单

- [ ] 使用 Java 21 运行时完成完整 `mvn clean verify`。
- [ ] TCP 开启角色鉴权，并使用独立、高熵的三个 token。
- [ ] 开启 TLS；需要客户端身份时开启 mTLS。
- [ ] 所有 secret 通过部署平台注入，不存在于 Git、普通配置响应和日志。
- [ ] namespace、nodeId 和 destination ID 在目标环境中唯一且稳定。
- [ ] ZooKeeper 至少三节点，并完成 SUSPENDED/LOST 和 Leader 切换演练。
- [ ] 多 Canal 节点使用 Agent FANOUT，并验证相同 requestId 到达所有副本。
- [ ] 每个节点使用独立持久化卷，容量至少覆盖峰值接入 72 小时。
- [ ] 下游按 `eventId` 幂等，接受 at-least-once 重复窗口。
- [ ] MQ 完成 Broker confirm、Broker 重启和“发送成功但 checkpoint 前崩溃”演练。
- [ ] 默认死信策略保持 `BLOCK`；启用 `SKIP` 前建立人工审批和告警。
- [ ] 监控 live、ready、destination 状态、delivery lag、WAL 磁盘和死信目录。
- [ ] 对 WAL、配置快照和 Admin 数据库建立备份及季度恢复演练。
- [ ] 进程管理器正确处理配置重启退出码 `20`。
- [ ] 使用生产 CA 验证证书轮换和未授权客户端拒绝。

## 常见问题

### `Address already in use`

TCP 11111、健康端口 11112 或 Admin 8080 已被占用。修改对应配置/环境变量，或停止占用端口的进程。

Windows：

```powershell
netstat -ano | Select-String ':11111|:11112|:8080'
```

Linux：

```bash
ss -lntp | grep -E '11111|11112|8080'
```

### Maven Wrapper 提示 Maven 未安装

本仓库 Wrapper 不会联网下载 Maven 二进制。确认 `.mvn/wrapper/apache-maven-3.9.9/` 存在，或者安装 Maven 3.8+ 后直接运行 `mvn clean verify`。

### `AUTHENTICATION_FAILED`

检查：

- HELLO 的角色名是否为 `AGENT_PRODUCER`、`DATA_CONSUMER` 或 `MONITOR`；
- token 是否对应所声明角色；
- Server 是否设置了全部三个 `CANAL_*_TOKEN`；
- 客户端是否在 TLS 连接上发送凭证。

### `REQUEST_ID_CONFLICT`

相同 Agent、destination 和 requestId 已经成功写入，但本次内容 checksum 不同。Agent 重试必须复用原 requestId 和完全相同的内容；新内容必须生成新的 requestId。

### `NOT_LEADER` 或 `STALE_EPOCH`

客户端连接到了 Follower，或连接建立后发生了 Leader 切换。重新发现当前节点并重新执行 SUBSCRIBE；不要复用旧订阅 epoch 提交 ACK。

### Server 启动时报告 WAL CRC 或历史段损坏

系统会 fail-closed，不会跳过历史数据。保留原目录，停止写入，从经过校验的备份恢复或执行受控修复；不要直接删除 `.log`。活动段只有尾部半写可以自动截断。

### 配置发布后没有重启

检查：

- `CANAL_ADMIN_BASE_URL` 和 namespace 是否匹配；
- Server 与 Admin 的 `CANAL_ADMIN_MACHINE_TOKEN` 是否相同；
- Admin 是否已经发布 release，而不是只创建 release；
- 进程管理器是否会在退出码 20 后重新拉起；
- `data/config/pending.json` 或 `rejected/` 是否出现新文件。

### Windows 构建无法删除 `*-all.jar`

Windows 不允许删除正在被 Java 进程加载的 JAR。先停止对应 Server/Admin 进程，再执行 `mvn clean verify`。不要直接终止所有 Java 进程，应按端口、启动命令或服务名确认目标。

### 看到 SLF4J `StaticLoggerBinder` 警告

部分第三方 MQ 客户端会在没有绑定时输出该提示。它不改变 WAL 和投递语义，但生产镜像应选择统一日志实现，并确认第三方库日志不会泄漏 token、URI 密码或 payload。

## 当前边界与后续工作

下面是当前实现的明确边界，不应在上线方案中被误解：

- ZooKeeper 不复制数据；多节点数据副本依赖 Agent FANOUT。
- 系统提供 at-least-once，不承诺跨本地 WAL、ZooKeeper 和任意 MQ 的 exactly-once。
- WAL 已支持分段、CRC、索引重建和恢复，但安全水位段清理目前由受控运维流程触发，尚无完整后台自动清理器。
- `/metrics` 是基础 Prometheus 文本端点，尚未覆盖文档列出的所有 destination、WAL、MQ 和配置轮询指标。
- Admin 当前是 API 服务，没有 Web UI。
- Admin 默认打包 H2 JDBC 驱动；切换其他数据库需要增加对应 JDBC 驱动并验证 SQL 方言。
- 配置变化通过整个进程退出和外部管理器重启生效，不支持 instance 热更新。
- TCP payload 当前使用 JSON；没有 Protobuf schema 协商。
- 部分样例调优字段为未来契约，当前固定参数见“当前固定运行时参数”。
- 真正上线前仍需在外部 ZooKeeper、Kafka、RocketMQ、RabbitMQ 和多节点环境完成故障注入与 24/72 小时稳定性测试。

详细完成度和发布门禁见 `docs/IMPLEMENTATION_STATUS.md`。

## 进一步阅读

- [完整开发与架构文档](docs/DEVELOPMENT.md)
- [逐章实现状态](docs/IMPLEMENTATION_STATUS.md)
- [TCP 协议摘要](docs/PROTOCOL.md)
- [运维、备份与恢复](docs/OPERATIONS.md)
- [编码规范](docs/CODING_STYLE.md)
- [ADR-0001 第一版实现基线](docs/adr/0001-baseline-decisions.md)
- [默认 Server 配置](canal-distribution/config/application.yaml)
- [Admin 环境变量样例](canal-distribution/admin.env.example)
- [Dockerfile](canal-distribution/Dockerfile)
- [Kubernetes StatefulSet](canal-distribution/k8s/canal-server.yaml)

---

如果只记住六条：**先写 WAL 再 ACK；只分发 EVENT_READY；一个 destination 只有一个出口；非 Leader 不供数；checkpoint 通过 version + epoch CAS 推进；下游必须按 eventId 幂等。**
