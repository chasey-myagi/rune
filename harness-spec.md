# Harness Spec: Rune Runtime Docker 化

## 概述

将 Rune Runtime（poc-v4）从"本地开发专用"升级为"可发布的 Docker 镜像"，使下游项目可以 `docker pull ghcr.io/chasey/rune-server` + `docker-compose up` 直接使用，无需 clone 源码。

## 目标

1. **配置外部化** — 运行时配置走环境变量，移除硬编码的示例 Rune/Flow 注册
2. **Dockerfile** — multi-stage build，产出精简镜像
3. **CI/CD** — GitHub Actions 自动构建并推送镜像到 ghcr.io

## Non-Goals

- 不做 Kubernetes/Helm 部署支持
- 不做配置文件（YAML/TOML）支持 — 环境变量足够当前阶段使用
- 不做 Rune/Flow 的动态注册 API（已有 Caster gRPC 机制处理远程 Rune）
- 不做镜像签名/SBOM
- 不改变现有 HTTP/gRPC API 接口

---

## P0: 配置外部化

### 现状

`rune-server/src/main.rs` 中硬编码了：

```rust
// 端口
let http_addr = "0.0.0.0:50060";
let grpc_addr = "0.0.0.0:50070";

// 示例 Rune（hello, step_b）— 发布版不该有
relay.register(RuneConfig { name: "hello".into(), ... }, ...);
relay.register(RuneConfig { name: "step_b".into(), ... }, ...);

// 示例 Flow（pipeline, single, empty）— 发布版不该有
flow_engine.register(Flow::new("pipeline")...);
flow_engine.register(Flow::new("single")...);
flow_engine.register(Flow::new("empty")...);
```

### 目标状态

发布版 `rune-server` 启动时是一个**干净的空壳 Runtime**：
- 没有预注册的本地 Rune
- 没有预注册的 Flow
- 端口通过环境变量配置，有合理默认值
- Caster 通过 gRPC 连入后动态注册远程 Rune

### 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `RUNE_HTTP_PORT` | `50060` | Gate HTTP API 监听端口 |
| `RUNE_GRPC_PORT` | `50070` | Caster gRPC 连接端口 |
| `RUNE_LOG_LEVEL` | `info` | 日志级别（trace/debug/info/warn/error） |
| `RUNE_HOST` | `0.0.0.0` | 监听地址 |

### 变更范围

**修改文件：** `rune-server/src/main.rs`

1. 删除所有 `relay.register(...)` 调用（hello, step_b）
2. 删除所有 `flow_engine.register(...)` 调用（pipeline, single, empty）
3. 端口读取改为 `std::env::var("RUNE_HTTP_PORT").unwrap_or("50060".into())`
4. 添加 `tracing_subscriber` 初始化，日志级别读取 `RUNE_LOG_LEVEL`
5. 启动时打印配置摘要（端口、日志级别）

**保留不变：**
- Relay、SessionManager、FlowEngine、GateState 的初始化逻辑
- gRPC SessionService 注册逻辑
- 所有 crate 内部代码（rune-core, rune-gate, rune-flow）

**新增文件：** `rune-server/src/config.rs`（可选，如果配置逻辑超过 10 行则提取）

### 示例代码迁移

原来 main.rs 中的示例 Rune/Flow 注册代码移到 `examples/` 目录，创建一个带注册的示例 server：

**新增文件：** `examples/demo-server/src/main.rs`
- 复制当前 main.rs 的完整逻辑（含示例注册）
- 供本地开发/演示使用
- 在 Cargo workspace members 中添加

### 验收标准

- [ ] `cargo run -p rune-server` 启动后，`GET /api/v1/runes` 返回空列表 `[]`
- [ ] `cargo run -p rune-server` 启动后，`GET /api/v1/flows` 返回空列表 `[]`
- [ ] `RUNE_HTTP_PORT=8080 RUNE_GRPC_PORT=9090 cargo run -p rune-server` 能在指定端口启动
- [ ] 不设环境变量时默认使用 50060/50070
- [ ] 启动日志显示监听端口和日志级别
- [ ] `cargo run -p demo-server`（examples）仍然能跑通原有的 hello/flow 演示

---

## P0: Dockerfile

### 构建策略

Multi-stage build：
1. **Builder stage** — `rust:1.85-bookworm`（或最新 stable），编译 release binary
2. **Runtime stage** — `debian:bookworm-slim`，仅包含运行时依赖

选择 Debian 而非 Alpine 的原因：Rust + gRPC（tonic/prost）在 musl 上的编译和运行时兼容性问题较多，Debian slim 镜像体积也在可接受范围（~80MB）。

### 文件位置

**新增文件：** `demo/poc-v4/Dockerfile`

### Dockerfile 设计

```dockerfile
# === Builder ===
FROM rust:1.85-bookworm AS builder

# 安装 protoc（rune-proto 编译需要）
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --release -p rune-server

# === Runtime ===
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/rune-server /usr/local/bin/rune-server

# 默认端口
ENV RUNE_HTTP_PORT=50060
ENV RUNE_GRPC_PORT=50070
ENV RUNE_LOG_LEVEL=info

EXPOSE 50060 50070

ENTRYPOINT ["rune-server"]
```

### 辅助文件

**新增文件：** `demo/poc-v4/.dockerignore`

```
target/
.git/
*.md
examples/
```

### 验收标准

- [ ] `docker build -t rune-server .` 在 `demo/poc-v4/` 目录下成功构建
- [ ] `docker run -p 50060:50060 -p 50070:50070 rune-server` 能正常启动
- [ ] `curl http://localhost:50060/health` 返回成功
- [ ] 镜像大小 < 150MB
- [ ] 支持通过 `-e RUNE_HTTP_PORT=8080` 覆盖端口配置

---

## P1: CI/CD（GitHub Actions）

### 触发条件

- **Push tag** `v*`（如 `v0.1.0`）→ 构建并推送带版本标签的镜像
- **Push to main** 的 `demo/poc-v4/**` 路径变更 → 构建并推送 `:latest` 镜像
- **手动触发**（workflow_dispatch）→ 构建并推送 `:dev` 镜像

### 镜像命名

```
ghcr.io/<owner>/rune-server:latest
ghcr.io/<owner>/rune-server:v0.1.0
ghcr.io/<owner>/rune-server:dev
```

> `<owner>` 从 `github.repository_owner` 自动获取。

### Workflow 设计

**新增文件：** `.github/workflows/docker-publish.yml`

```yaml
name: Docker Publish

on:
  push:
    tags: ['v*']
    branches: [main]
    paths: ['demo/poc-v4/**']
  workflow_dispatch:

env:
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository_owner }}/rune-server

jobs:
  build-and-push:
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - uses: actions/checkout@v4

      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - uses: docker/metadata-action@v5
        id: meta
        with:
          images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
          tags: |
            type=semver,pattern={{version}}
            type=raw,value=latest,enable={{is_default_branch}}
            type=raw,value=dev,enable=${{ github.event_name == 'workflow_dispatch' }}

      - uses: docker/build-push-action@v6
        with:
          context: demo/poc-v4
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
```

### 缓存策略

P1 阶段不做 Docker layer cache / cargo cache。首次构建约 5-10 分钟可接受。后续如有需要可加 `actions/cache` + `sccache`。

### 验收标准

- [ ] 推送 `v0.1.0` tag 后，`ghcr.io/<owner>/rune-server:v0.1.0` 可 pull
- [ ] 推送 main 分支的 poc-v4 变更后，`:latest` 标签更新
- [ ] 手动触发 workflow 后，`:dev` 标签更新
- [ ] Workflow 使用 `GITHUB_TOKEN` 认证，无需额外 secret

---

## P2: docker-compose 示例

### 目的

为下游项目（如 chasey-home）提供开箱即用的 compose 参考。

**新增文件：** `demo/poc-v4/docker-compose.yml`

```yaml
services:
  rune-server:
    image: ghcr.io/<owner>/rune-server:latest
    ports:
      - "50060:50060"
      - "50070:50070"
    environment:
      RUNE_HTTP_PORT: 50060
      RUNE_GRPC_PORT: 50070
      RUNE_LOG_LEVEL: info
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:50060/health"]
      interval: 10s
      timeout: 5s
      retries: 3
```

### 验收标准

- [ ] `docker-compose up` 能正常启动 rune-server
- [ ] health check 通过

---

## 实施顺序

```
Step 1: P0 配置外部化（修改 main.rs + 创建 demo-server example）
   ↓
Step 2: P0 Dockerfile（依赖 Step 1 的干净 main.rs）
   ↓
Step 3: P1 CI/CD（依赖 Step 2 的 Dockerfile）
   ↓
Step 4: P2 docker-compose 示例（依赖 Step 2）
```

## 成功标准

整体交付完成的判定：

1. **核心路径可用**：`docker run ghcr.io/<owner>/rune-server:latest` 启动后，Python Caster 能通过 gRPC 连入并注册 Rune，HTTP 客户端能调用已注册的远程 Rune
2. **下游可集成**：chasey-home 的 docker-compose.yml 能引用 rune-server 镜像并正常运行
3. **CI 自动化**：代码合入 main 或打 tag 后镜像自动发布到 ghcr.io
4. **开发体验不退化**：本地 `cargo run -p demo-server` 仍可跑完整演示

## 风险与注意事项

1. **protoc 依赖**：`rune-proto` 的 `build.rs` 需要 `protobuf-compiler`，Dockerfile builder stage 必须安装
2. **平台兼容性**：当前仅构建 linux/amd64。如需 ARM（Apple Silicon 用户本地跑），后续加 `platforms: linux/amd64,linux/arm64`
3. **镜像仓库权限**：首次推送 ghcr.io 包需要确认仓库的 packages 权限设置
4. **Cargo.lock**：必须 COPY 进 builder stage，确保可复现构建
