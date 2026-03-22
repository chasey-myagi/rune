# SDK Test Matrix

> 统一的 SDK 测试规范。Python SDK 和 TypeScript SDK 必须实现完全相同的测试用例。
> 新增 SDK 时，按此矩阵实现所有 case。

## 约定

- **UT** = 单元测试，不需要 runtime，测试 SDK 内部逻辑
- **E2E** = 端到端测试，需要运行 `rune-server --dev`，测试真实 gRPC 通信
- 每个 case 有唯一编号（U-xx / E-xx），两个 SDK 的测试名必须对应

## 前置条件

### UT
无。直接测试 SDK 类和方法。

### E2E
```bash
# 启动 dev server（绑定 localhost，禁用 auth，内存数据库）
cargo run -p rune-server -- --dev
# 等待 gRPC :50070 和 HTTP :50060 就绪
```

---

## Part 1: Unit Tests

### 1.1 类型和配置 (U-01 ~ U-10)

| ID | Case | 预期 |
|----|------|------|
| U-01 | RuneConfig 只传 name，其他字段取默认值 | version="0.0.0", supports_stream=false, gate=None, priority=0 |
| U-02 | RuneConfig 设置全部字段 | 全部字段正确存储 |
| U-03 | RuneConfig input_schema 为 JSON Schema dict/object | 正确存储 |
| U-04 | RuneConfig output_schema 为 JSON Schema dict/object | 正确存储 |
| U-05 | GateConfig 默认 method 为 POST | method == "POST" |
| U-06 | RuneContext 包含 rune_name, request_id, context | 字段可访问 |
| U-07 | CasterOptions/构造参数 默认 runtime 地址 | localhost:50070 |
| U-08 | FileAttachment 包含 filename, data, mime_type | 字段可访问 |
| U-09 | CasterOptions casterId 默认自动生成（不等于 key） | casterId != key |
| U-10 | CasterOptions casterId 自定义传入 | 使用传入的值 |

### 1.2 Rune 注册 (U-11 ~ U-20)

| ID | Case | 预期 |
|----|------|------|
| U-11 | rune() 注册 unary handler | 内部注册表含该 rune |
| U-12 | streamRune() 注册 stream handler | supports_stream=true |
| U-13 | 注册多个 rune（3个） | 全部在注册表中 |
| U-14 | 重复注册同名 rune | 抛错 / 覆盖（明确一种行为） |
| U-15 | 注册时设置 gate path | config.gate.path 正确 |
| U-16 | 注册时设置 input_schema | config.input_schema 正确 |
| U-17 | 注册时设置 output_schema | config.output_schema 正确 |
| U-18 | 注册时设置 priority | config.priority 正确 |
| U-19 | handler 可直接调用（不经 gRPC） | 返回预期结果 |
| U-20 | stream handler 可直接调用 | emit 被调用 |

### 1.3 StreamSender (U-21 ~ U-27)

| ID | Case | 预期 |
|----|------|------|
| U-21 | emit(bytes) 发送数据 | 不抛错 |
| U-22 | emit(string) 自动编码 | 不抛错 |
| U-23 | emit(object/dict) 自动 JSON 序列化 | 不抛错 |
| U-24 | end() 标记流结束 | 后续 emit 抛错 |
| U-25 | emit after end 抛错 | 抛出 StreamEnded 类错误 |
| U-26 | 多次 emit | 每次都成功 |
| U-27 | end() 幂等 | 多次 end 不抛错 |

### 1.4 连接配置 (U-28 ~ U-33)

| ID | Case | 预期 |
|----|------|------|
| U-28 | 重连初始延迟可配置 | 使用配置值 |
| U-29 | 重连最大延迟可配置 | 使用配置值 |
| U-30 | 心跳间隔可配置 | 使用配置值 |
| U-31 | maxConcurrent 可配置 | 使用配置值 |
| U-32 | labels 可配置 | 使用配置值 |
| U-33 | API key 传入 | 内部存储 |

### 1.5 FileAttachment 检测 (U-34 ~ U-37)

| ID | Case | 预期 |
|----|------|------|
| U-34 | handler(ctx, input) — 不接受文件 | accepts_files = false |
| U-35 | handler(ctx, input, files) — 接受文件 | accepts_files = true |
| U-36 | stream handler(ctx, input, stream) — 不接受文件 | accepts_files = false |
| U-37 | stream handler(ctx, input, files, stream) — 接受文件 | accepts_files = true |

---

## Part 2: E2E Tests

> 以下所有测试需要真实 `rune-server --dev` 运行。

### 2.1 连接与注册 (E-01 ~ E-06)

| ID | Case | 预期 |
|----|------|------|
| E-01 | Caster 连接到 runtime，注册 1 个 rune | GET /api/v1/runes 列表含该 rune |
| E-02 | Caster 连接后注册 3 个 rune | 列表含全部 3 个 |
| E-03 | Caster 注册带 gate_path 的 rune | 列表中 gate_path 正确 |
| E-04 | Caster 注册带 schema 的 rune | rune info 中 schema 存在 |
| E-05 | Caster 断开后 rune 从列表消失 | GET /api/v1/runes 不再含该 rune |
| E-06 | Caster 断开后自动重连 | 重连后 rune 重新出现 |

### 2.2 同步调用 (E-10 ~ E-16)

| ID | Case | 预期 |
|----|------|------|
| E-10 | POST /api/v1/runes/{name}/run — echo rune | 200, 返回输入的 JSON |
| E-11 | 通过 gate_path 调用 | 200, 同上 |
| E-12 | handler 修改输入并返回 | 200, 返回修改后的 JSON |
| E-13 | handler 抛异常 | 500, error 包含异常信息 |
| E-14 | 调用不存在的 rune | 404 |
| E-15 | 大 payload（100KB JSON） | 200, 正确往返 |
| E-16 | 空 body 调用 | 200, handler 收到空 bytes |

### 2.3 流式调用 (E-20 ~ E-25)

| ID | Case | 预期 |
|----|------|------|
| E-20 | POST ?stream=true — stream rune 发 3 个 chunk | SSE 收到 3 个 message 事件 + done |
| E-21 | stream handler emit string | SSE data 正确 |
| E-22 | stream handler emit JSON object | SSE data 是 JSON |
| E-23 | stream handler 抛异常 | SSE 收到 error 事件 |
| E-24 | 对非 stream rune 请求 stream | 400 |
| E-25 | stream 中途客户端断开 | handler 收到 cancel signal（如果支持） |

### 2.4 异步调用 (E-30 ~ E-35)

| ID | Case | 预期 |
|----|------|------|
| E-30 | POST ?async=true | 202, 返回 task_id |
| E-31 | GET /api/v1/tasks/{id} 查询完成的任务 | status=completed, 有 output |
| E-32 | GET /api/v1/tasks/{id} 查询失败的任务 | status=failed, 有 error |
| E-33 | DELETE /api/v1/tasks/{id} 取消运行中任务 | status=cancelled |
| E-34 | GET 不存在的 task_id | 404 |
| E-35 | 异步调用慢 rune，轮询等完成 | 最终 status=completed |

### 2.5 Schema 校验 (E-40 ~ E-45)

| ID | Case | 预期 |
|----|------|------|
| E-40 | 注册带 input_schema 的 rune，发合法 input | 200 |
| E-41 | 发不合法 input（缺必填字段） | 422, error 含字段名 |
| E-42 | 注册无 schema 的 rune，发任意 input | 200（跳过校验） |
| E-43 | input_schema + output_schema 都设置，全合法 | 200 |
| E-44 | output 不合法（handler 返回不符合 schema） | 500 |
| E-45 | OpenAPI 端点包含 schema 信息 | GET /api/v1/openapi.json 含该 rune |

### 2.6 文件传输 (E-50 ~ E-55)

| ID | Case | 预期 |
|----|------|------|
| E-50 | multipart 上传文件 + JSON → handler 收到 files | 200, 响应含 file metadata |
| E-51 | handler 收到的 FileAttachment 字段正确 | filename, mime_type, data 一致 |
| E-52 | 多文件上传 | handler 收到多个 FileAttachment |
| E-53 | 超大文件超限 | 413 |
| E-54 | 无文件纯 JSON（multipart 向后兼容） | 200 |
| E-55 | handler 不接受 files 参数时文件被忽略 | 200, 正常执行 |

### 2.7 心跳与生命周期 (E-60 ~ E-63)

| ID | Case | 预期 |
|----|------|------|
| E-60 | Caster 保持连接 30 秒，心跳正常 | 连接稳定 |
| E-61 | runtime 重启后 Caster 自动重连 | 重连成功，rune 重新注册 |
| E-62 | 并发调用同一 rune（10 次） | 全部返回正确结果 |
| E-63 | 两个 Caster 注册同名 rune | 负载均衡分配 |

---

## 实现指南

### Python E2E 测试

```python
import pytest
import subprocess
import httpx
import asyncio
import time

@pytest.fixture(scope="module")
def runtime():
    """启动 rune-server --dev，测试结束后关闭"""
    proc = subprocess.Popen(
        ["cargo", "run", "-p", "rune-server", "--", "--dev"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    # 等待就绪
    for _ in range(30):
        try:
            r = httpx.get("http://localhost:50060/health")
            if r.status_code == 200:
                break
        except:
            time.sleep(0.5)
    yield proc
    proc.terminate()
    proc.wait()

@pytest.fixture
async def caster(runtime):
    """创建 Caster 并连接"""
    from rune_sdk import Caster
    c = Caster("localhost:50070")
    # 注册测试 rune...
    task = asyncio.create_task(c.run())
    await asyncio.sleep(1)  # 等待连接
    yield c
    task.cancel()
```

### TypeScript E2E 测试

```typescript
import { Caster } from '../src/index';
import { beforeAll, afterAll } from 'vitest';
import { spawn, ChildProcess } from 'child_process';

let server: ChildProcess;

beforeAll(async () => {
  server = spawn('cargo', ['run', '-p', 'rune-server', '--', '--dev']);
  // 等待就绪
  await waitForHealth('http://localhost:50060/health');
});

afterAll(() => {
  server.kill();
});
```

### 测试命名规范

- Python: `test_u01_rune_config_defaults`, `test_e10_sync_call_echo`
- TypeScript: `it('U-01: RuneConfig defaults')`, `it('E-10: sync call echo')`
