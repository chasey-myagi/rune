# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Rune 是一个协议优先的多语言分布式函数执行框架（v1.0.0）。Client 通过 HTTP API 发起调用，经 Gate → Relay → Resolver → Invoker 链路，最终通过 gRPC 双向流（Rune Wire Protocol）到达 Caster（Python/TypeScript/Rust SDK 编写的函数执行端）。支持三种调用模式：同步、流式 (SSE)、异步 (task_id 轮询)。

## Build & Test Commands

```bash
# 构建整个工作区
cargo build

# 运行所有 Rust 测试（549+ 测试）
cargo test

# 运行单个 crate 测试
cargo test -p rune-core
cargo test -p rune-flow
cargo test -p rune-gate
cargo test -p rune-store
cargo test -p rune-cli

# 运行单个测试函数
cargo test -p rune-core -- test_name

# 性能基准测试
cargo bench

# 启动服务器（开发模式）
cargo run -p rune-server -- --dev

# Python SDK 测试
cd sdks/python && .venv/bin/pytest tests/ -v

# TypeScript SDK 测试
cd sdks/typescript && npm test

# 生成 proto 代码（Python SDK）
cd sdks/python && bash gen_proto.sh
```

## Architecture — Cargo Workspace (10 crates)

```
runtime/
  rune-proto   — Protocol Buffer 生成代码（tonic-build，.proto 在 proto/ 下）
  rune-core    — 核心抽象层：Relay（注册表）、Resolver（调度策略）、SessionManager（gRPC 流管理）、Invoker（本地/远程调用）、App（组装入口）
  rune-gate    — HTTP 网关层：Axum 路由、认证/频率限制中间件、FileBroker（文件传输）、SSE 流式、GateState（共享状态）
  rune-flow    — DAG 工作流引擎：FlowDefinition → 拓扑排序 → 并行/串行执行
  rune-store   — SQLite 持久化：异步任务结果、调用日志、API Key、快照
  rune-schema  — JSON Schema 校验 + OpenAPI 生成
  rune-server  — 入口 binary，组装所有组件启动 HTTP + gRPC 服务
  rune-cli     — CLI 工具（clap derive），通过 HTTP API 管理运行时
sdks/
  python/      — Python SDK（grpcio，setuptools，Python 3.10+）
  typescript/  — TypeScript SDK（@grpc/grpc-js，vitest）
  rust/        — Rust SDK
examples/      — 7 个端到端示例（rust-caster、python 基础/流式/异步、typescript）
```

## Key Architectural Concepts

- **Rune** — 一个可远程调用的函数单元，有 name、version、gate_path 等元数据
- **Caster** — SDK 端的 gRPC 客户端，连接到 Runtime 注册 Rune 并接收执行请求
- **Relay** — 全局注册表，维护 rune_name → invoker 的映射，支持动态路由索引和冲突检测
- **Resolver** — 从同名 Rune 的多个 Caster 实例中选择目标（round_robin/random/least_load/priority/label）
- **SessionManager** — 管理 Caster gRPC 双向流连接，负责心跳、信号量并发控制（max_concurrent）、超时/断连/取消时的状态清理
- **Gate** — HTTP 层，路由请求到 Relay，处理认证、频率限制、文件上传
- **FlowEngine** — DAG 编排，支持条件分支、并行执行、input_mapping

## Rune Wire Protocol (RWP)

定义在 `proto/rune/wire/v1/rune.proto`，双向 gRPC 流。关键消息：
- Caster → Runtime: CasterAttach, CasterDetach, ExecuteResult, StreamEvent, StreamEnd
- Runtime → Caster: AttachAck, ExecuteRequest, CancelRequest
- 双向: Heartbeat

## Protocol Guarantees

`docs/protocol-guarantees.md` 定义了 18 条行为保证（并发信号量严格、超时/取消/断连状态清理、路由冲突检测等）。对应的回归测试锁定在 `tests/README.md` 中。修改核心行为时务必对照这些保证。

## Configuration

运行时配置为 TOML 格式（`rune.toml`），支持环境变量覆盖（`RUNE_` 前缀）。详见 `docs/configuration.md`。`--dev` 标志启用开发模式（localhost 绑定、禁用认证、内存数据库）。

## Key Documentation

- `docs/dev-design.md` — 命名哲学、四层架构、协议设计
- `docs/protocol-guarantees.md` — 18 条行为保证契约
- `docs/configuration.md` — 配置参考
- `docs/api-reference.md` — HTTP API 端点参考
- `docs/cli.md` — CLI 命令参考
