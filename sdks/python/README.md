# Rune Python SDK

Official Python Caster SDK for the Rune framework.

## 安装

```bash
pip install rune-sdk
```

要求 Python >= 3.10。

## 快速开始

```python
from rune_sdk import Caster

caster = Caster("localhost:50070")

@caster.rune("echo", gate="/echo")
async def echo(ctx, input):
    return input

caster.run()
```

启动后 Runtime 自动暴露：
```
POST /echo              -- sync
POST /echo?stream=true  -- SSE streaming
POST /echo?async=true   -- async task
```

## Caster 配置

```python
caster = Caster(
    addr="localhost:50070",           # Runtime gRPC 地址
    caster_id="my-python-caster",     # Caster ID（默认 "python-caster"）
    max_concurrent=10,                # 最大并发处理数
    labels={"env": "prod", "gpu": "true"},  # Caster 标签（用于标签路由）
    api_key="rk_xxx",                # API Key（认证开启时需要）
    reconnect_base_delay=1.0,         # 重连初始延迟（秒）
    reconnect_max_delay=30.0,         # 重连最大延迟（秒）
    heartbeat_interval=10.0,          # 心跳间隔（秒）
)
```

## 注册 Rune

### Unary Handler

```python
@caster.rune(
    "translate",
    gate="/translate",                # HTTP 路由路径
    gate_method="POST",               # HTTP 方法（默认 POST）
    version="1.0.0",                  # 语义版本
    description="Translate text",     # 人类可读描述
    priority=10,                      # 优先级（越高越优先）
    input_schema={                    # JSON Schema 输入校验
        "type": "object",
        "properties": {
            "text": {"type": "string"},
            "lang": {"type": "string"}
        },
        "required": ["text", "lang"]
    },
    output_schema={                   # JSON Schema 输出校验
        "type": "object",
        "properties": {
            "translated": {"type": "string"}
        }
    },
)
async def translate(ctx, input):
    data = json.loads(input)
    result = do_translate(data["text"], data["lang"])
    return json.dumps({"translated": result})
```

### Streaming Handler

```python
@caster.stream_rune("generate", gate="/generate")
async def generate(ctx, input, sender):
    prompt = json.loads(input)["prompt"]
    for token in model.stream(prompt):
        await sender.emit(token)       # 发送一个 chunk
    # StreamEnd 自动发送
```

`StreamSender.emit()` 接受 `bytes`、`str`、`dict`、`list` 类型，自动序列化。

### File Handler

Handler 的第三个参数命名为 `files` 时，自动接收文件附件：

```python
@caster.rune("process-doc", gate="/process")
async def process_doc(ctx, input, files):
    # files: list[FileAttachment]
    for f in files:
        print(f.filename, f.mime_type, len(f.data))
    return json.dumps({"processed": len(files)})
```

`FileAttachment` 字段：
- `filename: str` -- 文件名
- `data: bytes` -- 文件内容
- `mime_type: str` -- MIME 类型

## Context

每次调用都会传入 `RuneContext`：

```python
@caster.rune("example", gate="/example")
async def example(ctx, input):
    print(ctx.rune_name)     # "example"
    print(ctx.request_id)    # "r-18e8f3a1b-0"
    print(ctx.context)       # {} (额外的 key-value 上下文)
    return input
```

## 运行与停止

```python
# 阻塞运行（自动重连）
caster.run()

# 从另一个线程停止
caster.stop()
```

## 开发

```bash
cd sdks/python
pip install -e ".[dev]"
pytest tests/test_unit.py       # 单元测试
pytest tests/test_e2e.py -m e2e # E2E 测试（需要运行中的 Runtime）
```
