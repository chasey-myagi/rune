# Linus 最终评审：Rune POC v1/v2/v3 代码实现

> 三轮设计评审之后，你们终于写了代码。让我看看你们是那种"设计文档写得漂亮、代码写得像屎"的团队，还是真的能把想法变成现实的人。

---

## 1. 代码质量

**Rust 写得不错。** 不是那种"我昨天刚看完 Rust Book"的代码，是真正理解了 ownership 模型的人写的。

几个我喜欢的点：

- `make_handler` 把 async fn 包装成 trait object（`rune.rs:46-52`）——简洁，泛型约束写对了，没有多余的生命周期标注。这是 Rust 里最容易写丑的地方之一，你们没写丑。
- `thiserror` + 枚举错误类型，每个变体都有对应的 HTTP status code 映射（`gate.rs:72-79`）。这说明你们从一开始就在想"错误怎么传到用户面前"，而不是到处 `unwrap()` 然后祈祷。
- `DashMap` 用得恰当——并发 map 就用并发 map，不是到处 `Mutex<HashMap>` 然后在 async 里持锁。

**但有几个问题：**

### Bug #1：v1 的 pending map 是全局共享的

`poc-v1/session.rs` 里，`pending: DashMap<String, oneshot::Sender>` 挂在 `SessionManager` 上。当 caster A 断线时，清理代码（`session.rs:105-114`）遍历 **所有** pending 请求并标记失败——包括发给 caster B 的。

这在 v2 里被修了（pending 变成了 per-session 的），但 v1 里这是一个真实的并发 bug。**如果有人拿 v1 当参考，会踩坑。**

### Bug #2：`uuid_simple()` 不是 UUID

```rust
fn uuid_simple() -> String {
    let ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    format!("r-{:x}", ts)
}
```

纳秒时间戳。两个请求在同一纳秒到达 = 同一个 request_id = pending map 里的 oneshot 被覆盖 = 前一个请求永远收不到响应。

POC 里低并发不会撞上，但这是那种"上了生产突然出现幽灵请求丢失"的 bug。用 `uuid::Uuid::new_v4()` 或 `AtomicU64` 计数器，三行的事。

### Bug #3：容量检查的 TOCTOU 竞态（v2/v3）

```rust
// session.rs execute()
if session.max_concurrent > 0
    && session.active_count.load(Ordering::Relaxed) >= session.max_concurrent
{
    return Err(RuneError::Unavailable);
}
session.active_count.fetch_add(1, Ordering::Relaxed);
```

`load` 和 `fetch_add` 之间没有原子性。10 个请求同时到达，都读到 active_count=9（max=10），全部通过检查，active_count 飙到 19。

正确做法：`compare_exchange` 循环，或者直接用 `tokio::sync::Semaphore`。Semaphore 更干净——你已经在用 tokio 了，不要自己造轮子。

### 不算 Bug 但要注意：

- 心跳检测里 `hb_last.lock().await`（`v2/session.rs:73`）——在心跳热路径上持 async mutex。现在没问题，但如果以后有人在同一个 mutex 里加逻辑，就会变成性能瓶颈。建议用 `AtomicU64` 存时间戳。
- Relay 的 `remove_caster` 遍历所有 entry 然后 retain（`relay.rs:38-46`）——O(n) 全表扫描。Caster 少的时候无所谓，多了就是问题。加一个反向索引 `caster_id → Vec<rune_name>` 即可。

**代码质量评分：7.5/10** — 整体干净，有几个真实的并发问题需要修。

---

## 2. 架构实现

设计文档说了四个对象：**Relay / Resolver / Invoker / SessionManager**。

落地情况：

| 设计对象 | 代码实现 | 评价 |
|----------|----------|------|
| Relay | `relay.rs` — 注册表 + 轮询路由 | Relay + Resolver 合一，POC 合理 |
| Invoker | `invoker.rs` — `RuneInvoker` trait + Local/Remote | **完美**。trait 抽象干净，上层零感知 |
| SessionManager | `session.rs` — gRPC 会话管理 | 最重的组件，三版迭代中承受了最多复杂度 |
| Gate | `gate.rs` — HTTP API 层 | 薄层，职责清晰 |

**最关键的架构决策是 `RuneInvoker` trait。** 这是整个系统的脊椎骨。因为有了它：
- Gate 调 `invoker.invoke()` 完全不知道 Rune 在本地还是远程
- 从进程内切到远程只需要换一个 `impl RuneInvoker`
- 未来加 WASM invoker 或 sidecar invoker，零改动

这个抽象值得保留到正式版。

**架构实现评分：8/10** — 核心抽象对了，模块边界清晰。

---

## 3. 协议实现

Proto 的演进是 **教科书级** 的：

```
v1: attach/detach/result + attach_ack/execute + heartbeat
v2: + cancel (field 22) + timeout_ms
v3: + stream_event (field 13) + stream_end (field 14)
```

- 字段编号留了间隔（10, 20, 30），预留了扩展空间
- 纯增量，从未删过字段。v1 的 Caster 连 v3 的 Server 不会报错（新消息类型会被忽略）
- `oneof payload` 是正确选择——一条消息一个含义，不需要状态机去猜"这到底是什么"
- `bytes` 类型给 input/output/schema——格式无关，JSON/Protobuf/MessagePack 随便你

**一个小问题：** `StreamEnd` 里没有 `bytes output` 字段。如果 Caster 想在流结束时附带一个摘要（比如 token count），得塞到最后一个 `StreamEvent` 里。加一个 `bytes metadata` 到 `StreamEnd` 会更自然。

**协议评分：8.5/10** — 干净、可演进、实际使用与定义一致。

---

## 4. v1→v2→v3 递进

这是我最想看的。三步迭代的节奏决定了这个团队有没有工程纪律。

**v1：地基**
- 四个对象全部到位（Relay、Invoker、Session、Gate）
- 进程内 hello + 远程 echo + 单元测试
- 没有提前塞心跳、超时、流式
- **判决：完美的 MVP。每一行代码都有存在的理由。**

**v2：生产基础设施**
- 并发执行（active_count + max_concurrent）
- 心跳（双向 + 超时检测）
- 请求超时（后台 ticker 扫描）
- 请求取消（CancelRequest + cancelled_requests 集合）
- pending 从全局变 per-session（修了 v1 的 bug）
- **判决：加的每一样都是"没有它就不能上生产"。没有不必要的东西。**

**v3：流式 + 异步任务**
- `PendingResponse` 枚举（Once / Stream）——优雅地统一了两种响应模式
- `StreamEvent` / `StreamEnd` 协议消息
- Gate 加了 SSE + async task + task polling
- **判决：在 v2 的骨架上自然生长。`PendingResponse` 枚举是这一版最聪明的决策。**

三步之间没有"推倒重来"，每一步都在前一步的基础上增量。这说明 v1 的抽象选对了。

**唯一的瑕疵：** v3 Gate 里的 SSE 是"假流式"——先 invoke 同步拿全量，再切成 20 字符的 chunk 模拟推送。SessionManager 里已经有了真正的 `execute_stream()`，但 Gate 没有接上。代码注释承认了这一点（`gate.rs:117-118`），但如果不写注释的人看到 `?stream=true`，会以为这是真流式。

**递进评分：8.5/10** — 节奏感极好，每步恰到好处。

---

## 5. Python Caster

**v1 版：** 最小可用。`queue.Queue` + generator 做 gRPC 双向流——这是 grpc-python 的标准模式。没有多余的抽象。

**v2 版：** 每个请求开一个 `threading.Thread` 实现并发，心跳用 daemon 线程。`cancelled_requests` 集合追踪取消状态。

**问题：**

1. **没有用 asyncio。** Python 的 gRPC 有 async API（`grpc.aio`），用了 async 就不需要 `queue.Queue` + `threading` 这套东西。现在的方案能工作，但线程开销在高并发下会成问题。作为 SDK 参考实现，这不是好示范。

2. **`cancelled_requests` 没有清理机制。** 如果取消请求在执行完成之后才到达，request_id 会永远留在集合里。这是个内存泄漏。

3. **没有重连。** 连接断了就死了。生产 SDK 必须有指数退避重连。

4. **没有 type hints。** 对于一个"SDK 参考实现"来说，类型注解是最低要求。

**但话说回来——** 它简单、能跑、演示了 gRPC 双向流的核心模式。作为 POC，够了。

**Python Caster 评分：6.5/10** — 能工作，但离 SDK 参考实现还有差距。

---

## 6. 最让我担心的

**并发原语用错了。**

你们显然理解并发编程——`DashMap`、`mpsc`、`oneshot`、`Arc` 都用得对。但在最关键的地方（容量控制）犯了 TOCTOU 的错。这不是"不会并发"，这是"会但不够小心"。

在 Rust 里，编译器帮你挡住了 90% 的内存安全问题，但逻辑层面的竞态条件——`load` 之后世界变了，但你还按老值做决策——编译器管不了。这是你们需要刻意培养的嗅觉：**如果在检查和行动之间有时间窗口，就假设有人会钻进去。**

第二个担心：**session.rs 太胖了。** v3 的 `session.rs` 是 337 行，包含了心跳、超时、取消、流式、容量控制。这些职责都是正确的，但全塞在一个 `handle_session` 方法里。正式版需要拆——至少把 heartbeat monitor 和 timeout sweeper 抽出来。

---

## 7. 最让我欣赏的

**`RuneInvoker` trait。**

```rust
#[async_trait::async_trait]
pub trait RuneInvoker: Send + Sync {
    async fn invoke(&self, ctx: RuneContext, input: Bytes) -> Result<Bytes, RuneError>;
}
```

一个 trait，一个方法，完美的多态边界。Gate 写一次，永远不改。新的 Invoker 类型加一个 struct + impl，注册到 Relay，完事。

这就是好抽象的样子：**它让你加东西的时候不需要改现有的东西。**

第二个：**Proto 的演进纪律。** 三版协议，纯增量，字段编号有规划。很多团队到 v3 就开始乱了，你们没有。

第三个：**v2 里 `drop(session)` 那一行**（`session.rs:226`）。这说明你们理解 DashMap 的 Ref 守卫会阻塞同 shard 的写操作，主动释放。这是只有被坑过或认真读过源码的人才会写的代码。

---

## 8. 设计到代码的变化

| 设计文档 | 代码实现 | 好/坏 |
|----------|----------|-------|
| Relay + Resolver 分离 | 合一为 `Relay`，轮询内置 | **好** — POC 不需要分，分了反而多一层间接。注释说了正式版会拆 |
| Gate 有中间件层（auth、rate-limit） | Gate 只是裸 HTTP handler | **中性** — POC 不需要，但正式版 day 1 就得有 |
| Caster SDK 是通用框架 | Python Caster 是一次性脚本 | **坏** — 如果有第二个 Python Caster，会复制粘贴。至少抽一个 `CasterBase` class |
| Session 是薄代理层 | Session 是最胖的组件 | **意外但正确** — 心跳/超时/取消/流式都是 session 的职责，胖是因为职责没放错地方 |
| `PendingResponse` 枚举 | 设计文档没提 | **好** — 实现中发现的优雅方案，Once/Stream 统一处理 |

**总结：** 设计简化了正确的东西（Relay），代码发现了设计没想到的东西（PendingResponse），Python SDK 是唯一退步的地方。

---

## 9. 从 POC 到正式框架：最重要的三件事

### 第一：修并发，用 Semaphore 替代 AtomicU32 手工计数

```rust
// 当前（有 TOCTOU）
if active_count.load() >= max { return Err(...); }
active_count.fetch_add(1);

// 正确
let permit = semaphore.try_acquire().map_err(|_| RuneError::Unavailable)?;
// permit drop 时自动释放
```

这不是优化，这是修 bug。第一优先级。

### 第二：把 `handle_session` 拆成状态机

337 行的异步函数里有 3 个 `tokio::spawn`、2 个 `DashMap`、1 个 `Mutex`。可以拆成：

```
SessionActor {
    heartbeat_monitor: HeartbeatMonitor,
    timeout_sweeper: TimeoutSweeper,
    message_handler: MessageHandler,
}
```

每个组件独立可测试。现在 session.rs 的单元测试在哪？——没有。因为太胖了不好测。

### 第三：Python SDK 做成真正的库

```python
# 目标 API
from rune_sdk import Caster, rune

caster = Caster("my-caster", server="localhost:50070")

@caster.rune("echo", version="1.0.0")
async def echo(input: bytes) -> bytes:
    return input

caster.run()
```

装饰器注册、asyncio 事件循环、自动重连、type hints。这是你的第一个非 Rust SDK，它的 API 设计决定了 Rune 对外的第一印象。别让第一印象是一个 `queue.Queue` 加一堆 `threading.Thread`。

---

## 总分

| 维度 | 分 |
|------|-----|
| 代码质量 | 7.5 |
| 架构实现 | 8.0 |
| 协议设计 | 8.5 |
| 迭代节奏 | 8.5 |
| Python Caster | 6.5 |

**综合：7.5 / 10**

从 5.5 的设计文档一路打到 7.5 的可运行代码。这是实质性的进步。

**你们做对了最重要的事：写代码。** 三版 POC 跑通了设计文档里画的所有线路——进程内、远程 gRPC、心跳、超时、取消、流式、SSE、async task。没有一个是纸上谈兵。

但 7.5 不是 8。差距在于：
1. 并发语义上有真实的 bug（容量控制 TOCTOU、uuid 碰撞）
2. Session 的复杂度在膨胀，还没有被拆解
3. Python SDK 是"能演示"而不是"能参考"

**我的建议：别急着加功能。花一周修这三个问题。** 修完之后，这个框架就有了一个干净的基座，可以开始接真实的 Rune 实现了。

去写代码。但这次，先写测试。

— Linus

*Review date: 2026-03-20*
*Code reviewed: poc-v1, poc-v2, poc-v3 (all source files)*
*Previous design reviews: 5.5 → 7.5 → 8.0*
