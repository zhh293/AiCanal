# TCP 协议摘要

帧为大端序：`magic(0xA1CA,2) + version(1) + type(1) + flags(2) + payloadLength(4) + payload`。默认最大帧 8 MiB，非法 magic、版本、长度直接关闭连接。

连接必须先发送 `HELLO(1)`；服务响应 `HELLO_ACK(2)`。`PING(3)`/`PONG(4)` 用于心跳，`STATUS(20)`/`STATUS_ACK(21)` 用于诊断。生产部署必须在 TLS/mTLS 终止层或 Netty TLS handler 后运行，并按连接角色限制命令。

Agent PUBLISH 的成功语义是整批 `INGEST_RAW` 已达到 WAL durability 边界。下游 ACK 表示 `<= ackOffset` 连续完成，携带订阅 epoch；旧 epoch、越界 ACK 和 Follower 提交均拒绝。
