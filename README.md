# Rune

**定义函数，获得 API + 工作流 + 分布式执行。语言无关。**

一套开放协议 + Rust 参考实现。你用任何语言写业务逻辑（Rune），框架帮你搞定其他一切。

## 30 秒理解

### 方式 1：Rust 库

```rust
let mut app = App::new();
app.rune(RuneConfig { name: "translate".into(), ..default() }, translate_handler);
app.run().await;
// POST /translate 已就绪
```

### 方式 2：独立二进制 + 任意语言 Caster

```bash
# 启动 Rune Server（下载二进制即可）
$ rune-server --config config.toml
```

```python
# Python Caster — 你的全部代码
from rune import Caster

caster = Caster("localhost:9090")

@caster.rune("translate", gate="/translate")
def translate(ctx, input):
    return {"result": deepl.translate(input["text"], input["lang"])}

caster.run()
```

启动后自动获得：

```
POST /translate                  ← 同步调用
POST /translate?stream=true      ← SSE 流式
POST /translate?async=true       ← 异步任务
GET  /openapi.json               ← OpenAPI 文档
GET  /health                     ← 健康检查
```

## 核心概念

| 名称 | 是什么 |
|------|--------|
| **Rune** | 一个函数。输入 bytes → 输出 bytes。任何语言实现。 |
| **Caster** | 远程执行者。通过 gRPC 连接，注册并执行 Rune。 |
| **Gate** | HTTP 入口。自动把 Rune 映射为 HTTP API。 |
| **Kit** | 模块。一组相关 Rune + 配置 + 生命周期。 |
| **Relay** | 注册 + 路由。记录所有 Rune 在哪，把请求中继过去。 |
| **Flow** | 工作流。多个 Rune 组成的 DAG 流水线。 |

## 它组合了什么

| 能力 | 传统方案 | Rune |
|------|---------|------|
| HTTP API | Gin / FastAPI / Express | Rune + Gate 声明 |
| Schema 校验 + OpenAPI | swag / 手写 | 自动 |
| 流式 SSE | 手写 | `?stream=true` |
| 异步任务 | Celery / Asynq + Redis | `?async=true` |
| 工作流编排 | Temporal / Airflow | `rune-flow` Kit |
| 分布式执行 | 自建 gRPC | Caster 协议 |
| 多语言 | 各工具各自的 SDK | 统一 gRPC 协议 |

## 文档

| 文档 | 说明 |
|------|------|
| [技术设计](docs/dev-design.md) | 完整设计：协议 / Rust 架构 / Trait / 请求流转 / POC |
| [代码评审](docs/linus-final-review.md) | POC v1/v2/v3 代码实现评审（8.5/10） |

## 状态

🚧 **设计完成，准备写 POC** — 600 行 Rust + 50 行 Python 跑通 end-to-end。
