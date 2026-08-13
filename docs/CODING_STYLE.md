# 编码规范

## 通用规则

- 所有文本文件使用 UTF-8；除 Windows 批处理文件外统一使用 LF。
- 文件末尾保留一个换行，不保留行尾空格。
- Java、XML、YAML、JSON 使用空格缩进，不使用 Tab。
- 命名应表达业务含义；避免无意义缩写、魔法值和超长方法。
- 密钥、令牌、完整 payload 不得写入源码、配置样例或日志。

## Java

- 以 Google Java Format 作为唯一格式来源，缩进为 2 个空格。
- 一个源文件只定义一个顶级类型，文件名与顶级类型一致。
- 包名全小写；类型使用 `UpperCamelCase`；方法、字段和局部变量使用 `lowerCamelCase`；常量使用 `UPPER_SNAKE_CASE`。
- import 由格式化工具整理；新代码不得使用无必要的通配符 import。
- 对外 API 优先使用不可变对象和防御性复制；异常必须包含稳定错误码，不静默吞掉数据错误。
- 数据可靠性路径必须保持 WAL-before-ACK、checkpoint 单调推进和 epoch fencing 不变量。

## Maven 与提交前检查

格式化全部源码：

```bash
./mvnw spotless:apply
./mvnw sortpom:sort -Dsort.createBackupFile=false
```

提交前必须执行：

```bash
./mvnw clean verify
```

`verify` 会运行测试并执行格式检查。格式不一致时构建失败，不允许通过手工压缩代码绕过。
