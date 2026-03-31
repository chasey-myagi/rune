# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Rune 是一个「定义函数，获得 API + 工作流 + 分布式执行」的框架。开放协议 + Rust 参考实现，支持任何语言通过 gRPC (Caster) 注册并执行函数 (Rune)。

当前版本：v0.2.0。CLI 已重构，Docker-first Runtime 管理，跨平台发布。

## 构建与测试

```bash
# 构建全部
cargo build

# 构建指定 crate
cargo build -p rune-server
cargo build -p rune-cli

# 运行所有测试
cargo test

# 运行指定 crate 测试
cargo test -p rune-core
cargo test -p rune-gate
cargo test -p rune-cli
cargo test -p rune-flow

# 运行单个测试
cargo test -p rune-flow -- test_engine_execute_chain

# 启动服务器（HTTP :50060, gRPC :50070）
cargo run -p rune-server -- --dev

# CLI 启动 Runtime（Docker 模式）
cargo run -p rune-cli -- start --dev
```

Python Caster（示例客户端）：
```bash
cd examples/python-caster && python main.py
```

## 架构

### 核心概念

| 概念 | 说明 |
|------|------|
| **Rune** | 最小执行单元，`(RuneContext, Bytes) → Result<Bytes, RuneError>` |
| **Caster** | 远程执行者，通过 gRPC 双向流 (`Session`) 连接 Runtime |
| **Gate** | HTTP 入口层，自动把 Rune 映射为 REST API（sync / SSE stream / async task） |
| **Relay** | 注册表 + 路由调度（Resolver 选择 Caster） |
| **Invoker** | 统一调用接口 trait，`LocalInvoker`（进程内）和 `RemoteInvoker`（gRPC）对上层透明 |
| **Flow** | DAG 工作流引擎，多个 Rune 按有向无环图执行 |
| **SessionManager** | gRPC 会话管理，处理心跳、超时、取消、流式、容量控制 |

### Workspace 结构

```
runtime/
├── rune-proto   → proto 定义 + tonic 生成代码（Layer 0）
├── rune-core    → Rune / Invoker / Relay / SessionManager（Layer 1）
├── rune-gate    → HTTP API 层，axum 路由（Layer 2）
├── rune-flow    → DAG 工作流引擎（Layer 3 Extension）
├── rune-schema  → JSON Schema 校验 + OpenAPI 生成
├── rune-store   → SQLite 持久化（任务, 日志, Key）
├── rune-server  → Runtime 入口二进制
└── rune-cli     → CLI 工具

sdks/
├── python       → Python Caster SDK (PyPI: rune-sdk)
├── typescript   → TypeScript Caster SDK (NPM: @rune-sdk/caster)
└── rust         → Rust Caster SDK

examples/        → 示例代码（Python/TypeScript/Rust Caster）
proto/           → Wire Protocol 定义 (.proto)
```

### 关键抽象

**`RuneInvoker` trait** (`rune-core/src/invoker.rs`) — 整个系统的脊椎骨。Gate/Flow 调用 `invoker.invoke()` 完全不知道 Rune 在本地还是远程。新增 Invoker 类型只需 `impl RuneInvoker`。

**`PendingResponse` enum** (`rune-core/src/session.rs`) — 用 `Once(oneshot)` / `Stream(mpsc)` 统一普通响应和流式响应的处理。

### 请求流转

```
HTTP Request → Gate (axum)
  → Relay.resolve(rune_name) → 返回 Arc<dyn RuneInvoker>
  → invoker.invoke(ctx, input)
    → LocalInvoker: 直接调 handler
    → RemoteInvoker: SessionManager.execute() → gRPC 双向流 → Caster
```

### gRPC 协议 (Rune Wire Protocol)

`proto/rune/wire/v1/rune.proto` — 单一 `Session` 双向流，用 `oneof payload` 区分消息类型。字段编号留间隔 (10/20/30) 预留扩展。协议纯增量演进，从未删字段。

## Docker

```bash
# 本地构建镜像（从项目根目录）
docker build -t rune-server:local .

# 运行
docker run -d -p 50060:50060 -p 50070:50070 rune-server:local

# 验证
curl http://localhost:50060/health  # => ok
```

镜像发布到 `ghcr.io/chasey-myagi/rune-server`，CI 由 `.github/workflows/docker-publish.yml` 驱动。

环境变量：`RUNE_SERVER__HTTP_HOST`(0.0.0.0) / `RUNE_SERVER__HTTP_PORT`(50060) / `RUNE_SERVER__GRPC_PORT`(50070) / `RUNE_LOG__LEVEL`(info)

## 发版流程

```bash
# 1. 创建分支 + 提交
git checkout -b feat/xxx && git add ... && git commit

# 2. 推送并创建 PR
git push -u origin feat/xxx && gh pr create --title "..." --body "..."

# 3. 合并（main 分支受保护，需 admin bypass）
gh api repos/chasey-myagi/rune/branches/main/protection/enforce_admins -X DELETE
gh pr merge <PR号> --squash --admin --delete-branch
gh api repos/chasey-myagi/rune/branches/main/protection/enforce_admins -X POST

# 4. 同步本地 + 打 tag 发版
git checkout main && git pull --rebase origin main
git tag -a v0.x.x -m "描述"
git push origin v0.x.x
```

tag `v*` 推送后 CI 自动：
- 构建多架构 Docker 镜像（amd64 + arm64）发布到 ghcr.io
- 跨平台编译 CLI + Runtime 二进制，上传到 GitHub Releases
