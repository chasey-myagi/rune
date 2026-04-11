# Sage — Rune 生态的 AI Agent 框架

> 把 AI Agent 变成 Rune 生态中的一等公民 —— 可注册、可路由、可编排、Tool 即 Caster。

**状态：** 设计讨论中（2026-03-25）

---

## 1. 动机

Rune v1.0 是一个分布式函数执行框架。但在评估验证项目时，我们发现：小型项目（LearnKit、mytr 等）不需要 Rune 的能力，简单的 Axum / Express 就够了。

真正能体现 Rune 价值的场景是 **AI Agent 系统**：

- 多步骤管线（解析 → 检索 → 生成 → 输出）→ **DAG Flow**
- 多语言（Python ML 生态 + Rust 性能 + TS 前端）→ **多语言 SDK**
- 流式输出（LLM 逐 token）→ **Stream SSE**
- 长任务（文档处理、批量推理）→ **Async Task**
- GPU 背压（推理并发控制）→ **max_concurrent**
- 多模型路由（Claude / OpenAI / 本地模型）→ **标签路由**

因此我们决定为 Rune 构建 **Sage** —— 一种内置 ReAct 循环的特殊 Caster，让开发者只需定义业务相关的 Skill 和 Tool，即可获得一个完整的分布式 AI Agent。

---

## 2. 核心哲学：Everything is a Caster

Rune 的世界里只有一种原语：**Caster**（通过 Wire Protocol 注册函数的执行体）。

```
Sage           = 一个会思考的 Caster（内置 ReAct loop + 上下文管理）
Tool           = 一个普通 Caster（可执行动作：read/bash/edit/write 或自定义）
Skill          = 一个提供知识+指令的 Caster（待细化，见第 6 节）
LLM Provider   = 一个系统级 Caster（提供推理能力，见第 7 节）
```

没有例外。全部通过 Rune Wire Protocol 注册、路由、调度。

这意味着：
- Agent 调 LLM 是调一个 Rune（`llm/chat`），不是传 api_key
- Agent 调 Tool 是调一个 Rune，可以在任何语言、任何机器上
- Agent 的 Tool 可以是另一个 Sage（Agent 调 Agent）
- 所有调用都自动获得 Rune 的路由、限流、日志、持久化能力

---

## 3. 命名体系

| 概念 | 名称 | 含义 |
|------|------|------|
| 框架/最小执行单元 | **Rune** | 古北欧符文 —— 一段可调用的力量 |
| 远程执行者 | **Caster** | 施法者 —— 执行 Rune 的进程 |
| AI Agent | **Sage** | 贤者 —— 会思考、会使用工具的 Caster |
| HTTP 入口 | **Gate** | 门 |
| 注册表 | **Relay** | 中继 |
| 调度器 | **Resolver** | 选择目标 |
| 工作流 | **Flow** | DAG 编排 |

**选 Sage 而不是 Agent 的理由：**
- `Agent` 太泛，每个 AI 框架都叫 Agent，没有辨识度
- `Sage` 在 Rune 的北欧魔法世界观里自洽
- `from rune import Sage` 一看就知道是 Rune 生态，不和 LangChain/CrewAI 混淆
- 短、好记、好打

---

## 4. SDK 设计

### 4.1 命名规则

- Python 包名：`rune-sdk`，import 为 `rune`
- TypeScript 包名：`@rune-sdk/core`，import 为 `rune`
- Rust crate：`rune-sdk`

Sage 是 SDK 的一部分，不是独立包：

```python
# Python
from rune import Caster, Sage

# TypeScript
import { Caster, Sage } from 'rune';

# Rust
use rune_sdk::{Caster, Sage};
```

### 4.2 开发者体验目标

```python
from rune import Sage

sage = Sage(
    name="code-reviewer",
    gate="/review",
    runtime="localhost:50070",

    # 业务配置 — 开发者只需要关心这些
    system_prompt="你是一个代码审查专家...",
    skills=["review-guidelines"],       # Skill Caster 引用
    files=["./context/standards.md"],    # 静态知识文件

    # Tool = 其他 Caster 的引用
    tools=["search_codebase", "run_tests"],
)

sage.run()
```

注册后自动获得：
- `POST /review` — 同步调用
- `POST /review?stream` — SSE 流式（逐 token + tool 执行过程）
- `POST /review?async` — 异步任务

---

## 5. Tool 体系

### 5.1 四大内置 Tool

参考 pi-coding-agent 的实现，Sage 内置四个基础 Tool：

| Tool | 功能 | 关键特性 |
|------|------|---------|
| **read** | 读文件（文本 + 图片） | 大文件截断 + offset/limit 分页 |
| **bash** | 执行 shell 命令 | 流式输出 + timeout + abort |
| **edit** | 精确查找替换 | 模糊匹配兜底 + 并发写入队列 |
| **write** | 创建/覆盖文件 | 自动创建目录 |

每个内置 Tool 通过可插拔的 **Operations 接口** 实现沙箱隔离：

```python
# 默认：本地文件系统 + 本地 shell
# 可替换为：SSH 远程执行、容器内执行、云存储读写等
```

### 5.2 Tool = Caster

自定义 Tool 就是普通的 Rune 函数。开发者在任何语言中注册一个 Caster，Sage 就能把它当 Tool 调用：

```python
# 某个独立的 Caster（可以是 Python/TS/Rust）
from rune import Caster

caster = Caster("localhost:50070")

@caster.rune("search_codebase", gate="/tools/search")
async def search(ctx, input):
    # Rust 级性能的代码搜索
    return results
```

```python
# Sage 引用它作为 Tool
sage = Sage(
    tools=["search_codebase"],  # 引用上面注册的 Rune
    ...
)
```

Sage 调 Tool 的过程：
```
Sage 决定调用 search_codebase
  → 通过 Rune Wire Protocol 调用（和调任何 Rune 一样）
  → Relay 查找 → Resolver 选择 → 请求到达目标 Caster
  → 结果返回 Sage → 继续 ReAct loop
```

### 5.3 Tool 调 Agent（递归组合）

Tool 列表里可以引用另一个 Sage：

```python
sage_coder = Sage(name="coder", ...)
sage_reviewer = Sage(name="reviewer", ...)

sage_lead = Sage(
    name="tech-lead",
    tools=["coder", "reviewer"],  # 两个都是 Sage
    ...
)
```

tech-lead 可以把任务委托给 coder 或 reviewer，它们各自有自己的 Tool 和 Skill。**这在 pi-coding-agent 和现有 Agent 框架里做不到。**

---

## 6. Skill 体系（待细化）

### 6.1 现状理解

Skill ≠ 一个 .md 文件。Skill 的数据本质是一个 **结构化的能力包**（可能包含指令、知识、示例、配置）。

### 6.2 设计方向

遵循「Everything is a Caster」原则，Skill 也应该是一个 Caster。但 Skill 和 Tool 有本质区别：

- **Tool** = 可执行动作（输入 → 执行 → 输出）
- **Skill** = 知识 + 指令集（影响 Agent 的行为模式和决策）

Skill 作为 Caster 可能的形态：
- Sage 启动时调用 Skill Caster 获取 system prompt 片段 / guidelines / 示例
- Skill Caster 可以动态返回内容（根据上下文/版本/A-B 测试）
- Skill 的打包格式可能是 zip（包含多个文件 + manifest）

### 6.3 待讨论问题

- Skill Caster 的接口定义是什么？（输入什么、返回什么？）
- Skill 和 Tool 的边界在哪？（有些能力既是知识又是动作）
- Skill 的生命周期？（启动时加载一次 vs 每次对话动态查询）
- Skill 的打包 / 分发 / 版本管理机制？

**此节需要进一步讨论后确定。**

---

## 7. LLM Provider — 系统级 Caster

### 7.1 设计理念

LLM 调用是基础设施，不是业务配置。Agent 开发者不应该碰 API key。

LLM Provider 是一种**系统级 Caster**，由管理员注册和管理：

```bash
# 管理员操作
rune provider add claude --api-key sk-xxx --models sonnet,opus
rune provider add openai --api-key sk-xxx --models gpt-4o
rune provider list
```

### 7.2 注册后自动暴露的 Rune

| Rune | 功能 |
|------|------|
| `llm/chat` | 对话补全（流式） |
| `llm/embed` | 文本向量化 |
| `llm/models` | 可用模型列表 |

### 7.3 Sage 调 LLM 的流程

```
Sage 需要 LLM 响应
  → 调用 "llm/chat" Rune（和调任何 Tool 一样）
  → Relay 查找 → Resolver 按 model 标签路由
  → 请求到达 LLM Provider Caster
  → Provider 调 Claude/OpenAI API → 流式返回
  → Sage 收到响应 → 继续 ReAct loop
```

### 7.4 多 Provider 调度

通过 Rune 已有的 Resolver 机制实现：

- **标签路由**：`model=claude-sonnet` → 路由到 Claude Provider
- **负载均衡**：同一 Provider 注册多个 API key，轮询分发
- **Fallback**：Claude 不可用时自动切换到 OpenAI
- **背压**：`max_concurrent` 控制并发推理数

### 7.5 混合模式（兼容）

默认走 Runtime 统一管理。但保留本地模式作为开发/调试快捷方式：

```python
# 生产模式（推荐）— 不传任何 LLM 配置
sage = Sage(name="reviewer", ...)

# 开发模式（本地调试）— 显式指定
sage = Sage(
    name="reviewer",
    llm_provider="local",
    llm_api_key="sk-...",
    llm_model="claude-sonnet-4-20250514",
    ...
)
```

### 7.6 待讨论问题

- Provider Caster 的具体实现形态？（内置到 Runtime？还是独立进程？）
- Provider 配置的持久化？（rune.toml？SQLite？CLI 管理？）
- 用量统计和计费的粒度？（per-agent? per-request? per-token?）
- Provider 的认证隔离？（不同 Agent 能否访问不同的 Provider pool？）

---

## 8. 架构全景

```
开发者                        Rune Runtime                       基础设施
────────                     ─────────────                      ──────────

Sage                    ┌─── Gate (HTTP) ──────┐
 ├─ system_prompt       │                      │
 ├─ skills ─────────────┼──→ Relay ──→ Resolver │
 ├─ tools ──────────────┼──→   ↓         ↓     │         LLM Provider Caster
 └─ files               │   Invoker            │          ├─ Claude
                        │     ↓                │          ├─ OpenAI
Tool Caster A ──────────┼──→ SessionManager    │          └─ Local Model
Tool Caster B ──────────┤   (心跳/信号量/重连)   │
Skill Caster C ─────────┤                      │         Storage
                        └──────────────────────┘          └─ SQLite (任务/日志/Key)

所有箭头都是 gRPC 双向流 (Rune Wire Protocol)
```

### 与 pi-coding-agent 的差异化

| | pi-coding-agent | Rune Sage |
|---|---|---|
| 部署 | 本地 CLI 单进程 | 分布式注册到 Runtime，多实例 |
| Tool 来源 | 进程内 JS 模块 | **任意 Caster**（跨语言、跨机器） |
| LLM 管理 | 每个 Agent 自带 API key | **系统级 Provider Caster** |
| 路由 | 无 | 标签路由 + 多策略调度 |
| 伸缩 | 单用户 | 多用户并发 + 背压 |
| 编排 | 单 Agent | **Flow DAG 多 Agent 协作** |
| 输出 | 终端 CLI | SSE 流式 → 任何前端 |
| Skill | 文件系统 | **Caster 交付**（动态、版本化） |

---

## 9. 开放问题清单

| # | 问题 | 优先级 |
|---|------|--------|
| 1 | Skill Caster 的接口定义和生命周期 | P0 |
| 2 | LLM Provider Caster 的具体实现形态 | P0 |
| 3 | Sage 的上下文管理机制（token 窗口、历史压缩） | P1 |
| 4 | Sage 的 ReAct loop 具体实现（基于 pi-coding-agent 还是重写） | P1 |
| 5 | 沙箱隔离的实现方式（Operations 接口 vs 容器） | P1 |
| 6 | Skill 的打包 / 分发 / 版本管理机制 | P2 |
| 7 | Provider 认证隔离和用量计费 | P2 |
| 8 | 多 Sage 协作的 Flow 编排模式 | P2 |
