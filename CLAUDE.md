# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

Rune 是一个「定义函数，获得 API + 工作流 + 分布式执行」的框架。开放协议 + Rust 参考实现，支持任何语言通过 gRPC (Caster) 注册并执行函数 (Rune)。

当前状态：POC 阶段（demo/poc-v1 到 poc-v4 四版迭代），尚未进入正式框架开发。

## 构建与测试

```bash
# 构建某个 POC（以最新 v4 为例）
cd demo/poc-v4 && cargo build

# 运行所有测试
cd demo/poc-v4 && cargo test

# 运行单个 crate 的测试
cd demo/poc-v4 && cargo test -p rune-core
cd demo/poc-v4 && cargo test -p rune-flow

# 运行单个测试
cd demo/poc-v4 && cargo test -p rune-flow -- test_engine_execute_chain

# 启动服务器（HTTP :50060, gRPC :50070）
cd demo/poc-v4 && cargo run -p rune-server

# Proto 编译（tonic-build，cargo build 时自动执行）
# proto 源文件在 demo/poc-vN/proto/rune/wire/v1/rune.proto
```

Python Caster（示例客户端）：
```bash
cd demo/poc-v4/examples/python-caster && python caster.py
```

## 架构

### 核心概念

| 概念 | 说明 |
|------|------|
| **Rune** | 最小执行单元，`(RuneContext, Bytes) → Result<Bytes, RuneError>` |
| **Caster** | 远程执行者，通过 gRPC 双向流 (`Session`) 连接 Runtime |
| **Gate** | HTTP 入口层，自动把 Rune 映射为 REST API（sync / SSE stream / async task） |
| **Relay** | 注册表 + 轮询路由（POC 中 Relay + Resolver 合一，正式版将拆分） |
| **Invoker** | 统一调用接口 trait，`LocalInvoker`（进程内）和 `RemoteInvoker`（gRPC）对上层透明 |
| **Flow** | 工作流引擎，多个 Rune 按 Chain 顺序执行 |
| **SessionManager** | gRPC 会话管理，处理心跳、超时、取消、流式、容量控制 |

### Workspace 结构（以 poc-v4 为例）

```
rune-proto   → proto 定义 + tonic 生成代码（Layer 0）
rune-core    → Rune / Invoker / Relay / SessionManager（Layer 1）
rune-gate    → HTTP API 层，axum 路由（Layer 2）
rune-flow    → Flow DSL + Chain 执行引擎（Layer 3 Extension）
rune-server  → 组装一切的二进制入口
examples/    → hello (Rust) + python-caster (Python)
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

## POC 版本差异

- **v1**: 基础骨架（Relay / Invoker / Session / Gate）
- **v2**: 并发控制、心跳、超时、取消、per-session pending
- **v3**: 流式（StreamEvent/StreamEnd）、SSE、async task
- **v4**: Flow 引擎（Chain DAG）、Flow API 端点

每个 poc-vN 是独立完整的 workspace，可独立构建运行。

## Docker

```bash
# 本地构建镜像
cd demo/poc-v4 && docker build -t rune-server:local .

# 运行
docker run -d -p 50060:50060 -p 50070:50070 rune-server:local

# 验证
curl http://localhost:50060/health  # => ok
```

镜像发布到 `ghcr.io/chasey-myagi/rune-server`，CI 由 `.github/workflows/docker-publish.yml` 驱动。

环境变量：`RUNE_HOST`(0.0.0.0) / `RUNE_HTTP_PORT`(50060) / `RUNE_GRPC_PORT`(50070) / `RUNE_LOG_LEVEL`(info)

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

tag `v*` 推送后 CI 自动构建多架构镜像（amd64 + arm64）并发布到 ghcr.io。

## 已知问题（来自代码评审）

- v3 Gate 的 SSE 是「假流式」——先同步拿全量再切 chunk 模拟推送，未接入 SessionManager 的真 `execute_stream()`
- `session.rs` 的 `handle_session` 方法职责过重（心跳/超时/取消/流式/容量控制），正式版需拆分为独立组件
- Relay 的 `remove_caster` 是 O(n) 全表扫描，正式版需加反向索引
