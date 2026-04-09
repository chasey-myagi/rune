# Rune API Reference

所有 HTTP 端点的完整参考。默认监听 `http://0.0.0.0:50060`。

## 认证

当 `auth.enabled = true`（默认）时，所有请求必须携带 API Key：

```
Authorization: Bearer rk_xxxxxxxxxxxxxxxx
```

免认证路由通过 `auth.exempt_routes` 配置，默认为 `["/health"]`。

开发模式（`rune start --dev`）跳过所有认证。

---

## 健康检查

### `GET /health`

检查 Runtime 是否在线。drain 期间返回 503。

**认证**: 不需要（默认免认证）

**响应**:
- `200 OK` -- body: `ok`
- `503 Service Unavailable` -- body: `draining`（优雅停机期间）

---

## Rune 调用

### `POST /api/v1/runes/{name}/run`

通过 debug 端点调用指定 Rune。适用于没有声明 `gate.path` 的 Rune。

**路径参数**:
- `name` -- Rune 名称

**查询参数**:
- `stream=true` -- 启用 SSE 流式模式
- `async=true` -- 启用异步模式

**请求头**:
- `Content-Type: application/json` -- JSON 输入
- `Content-Type: multipart/form-data` -- 文件上传（含可选 JSON input 字段）
- `X-Rune-Labels: key1=value1,key2=value2` -- 标签路由（可选）

**请求体**: Rune 的输入数据（JSON 或 multipart）

#### Sync 模式（默认）

**响应**:
- `200 OK` -- Rune 输出（JSON 或 raw bytes）
- `404 NOT_FOUND` -- Rune 不存在
- `422 VALIDATION_FAILED` -- 输入 schema 校验失败
- `500 OUTPUT_VALIDATION_FAILED` -- 输出 schema 校验失败
- `503 NO_MATCHING_CASTER` -- 标签路由无匹配

```json
{"result": "translated text"}
```

带文件上传时：
```json
{
  "result": "processed",
  "files": [
    {
      "file_id": "f-abc123",
      "filename": "doc.pdf",
      "mime_type": "application/pdf",
      "size": 12345,
      "transfer_mode": "broker"
    }
  ]
}
```

#### Stream 模式（`?stream=true`）

**响应**: `200 OK`，Content-Type: `text/event-stream`

SSE 事件：
```
event: message
data: chunk content

event: done
data: [DONE]
```

错误时：
```
event: error
data: error message
```

**注**: Rune 必须声明 `supports_stream = true`，否则返回 `400 STREAM_NOT_SUPPORTED`。

#### Async 模式（`?async=true`）

**响应**: `202 Accepted`

```json
{
  "task_id": "r-18e8f3a1b-0",
  "status": "running"
}
```

### `{gate_path}` -- 动态业务路由

Rune 声明 `gate.path` 后自动注册为真实 HTTP 路由。行为与 `/api/v1/runes/{name}/run` 完全一致，支持同样的查询参数和请求头。

**示例**: Rune 声明 `gate.path = "/translate"` 后：
```
POST /translate              -- sync 调用
POST /translate?stream=true  -- SSE 流式
POST /translate?async=true   -- 异步任务
```

---

## 异步任务

### `GET /api/v1/tasks`

列出异步任务。支持按状态、Rune 名称过滤和分页。

**查询参数**:
- `status` -- 按任务状态过滤（可选）。取值：`pending` | `running` | `completed` | `failed` | `cancelled`
- `rune` -- 按 Rune 名称过滤（可选）
- `limit` -- 返回条数，默认 50，最大 500（可选）
- `offset` -- 偏移量，默认 0（可选）

**响应**: `200 OK`

```json
{
  "tasks": [
    {
      "task_id": "r-18e8f3a1b-0",
      "rune_name": "translate",
      "status": "completed",
      "input": "{\"text\": \"hello\"}",
      "output": "{\"translated\": \"你好\"}",
      "error": null,
      "created_at": "2026-03-23T10:00:00Z",
      "started_at": "2026-03-23T10:00:00Z",
      "completed_at": "2026-03-23T10:00:01Z"
    }
  ]
}
```

### `GET /api/v1/tasks/{id}`

查询异步任务状态。

**路径参数**:
- `id` -- 任务 ID（由 async 模式返回的 `task_id`）

**响应**:
- `200 OK`

```json
{
  "task_id": "r-18e8f3a1b-0",
  "rune_name": "translate",
  "status": "completed",
  "input": "{\"text\": \"hello\"}",
  "output": "{\"translated\": \"你好\"}",
  "error": null,
  "created_at": "2026-03-23T10:00:00Z",
  "started_at": "2026-03-23T10:00:00Z",
  "completed_at": "2026-03-23T10:00:01Z"
}
```

- `404 NOT_FOUND` -- 任务不存在

**status 取值**: `pending` | `running` | `completed` | `failed` | `cancelled`

### `DELETE /api/v1/tasks/{id}`

取消运行中的异步任务。

**路径参数**:
- `id` -- 任务 ID

**响应**:
- `200 OK` -- 任务已取消

```json
{"task_id": "r-18e8f3a1b-0", "status": "cancelled"}
```

- `404 NOT_FOUND` -- 任务不存在
- `409 CONFLICT` -- 任务已完成或已失败，无法取消

---

## 管理 API

### `GET /api/v1/runes`

列出所有在线 Rune。

**响应**: `200 OK`

```json
{
  "runes": [
    {"name": "translate", "gate_path": "/translate"},
    {"name": "internal-rune", "gate_path": null}
  ]
}
```

### `GET /api/v1/status`

查看 Runtime 状态。

**响应**: `200 OK`

```json
{
  "uptime_secs": 3600,
  "caster_count": 3,
  "rune_count": 5,
  "dev_mode": false
}
```

### `GET /api/v1/casters`

列出在线 Caster。

**响应**: `200 OK`

```json
{
  "casters": [
    {
      "caster_id": "python-caster-1",
      "runes": ["translate", "summarize"],
      "role": "caster",
      "max_concurrent": 10,
      "available_permits": 8,
      "pressure": 0.2,
      "metrics": {
        "active_requests": 2,
        "max_concurrent": 10,
        "available_permits": 8
      },
      "health_status": "HEALTHY",
      "connected_since": 3600
    }
  ]
}
```

`available_permits` 取代旧的 `current_load` 字段名；同时新增 `role`、`max_concurrent`、`pressure`、`metrics`、`health_status`。

### `GET /api/v1/stats`

查看调用统计。

### `GET /api/v1/stats/casters`

查看按 Caster 聚合的调用统计。

**响应**: `200 OK`

```json
{
  "casters": [
    {
      "caster_id": "python-caster-1",
      "count": 128,
      "avg_latency_ms": 42,
      "success_rate": 0.992,
      "p95_latency_ms": 91.0
    }
  ]
}
```

### `GET /api/v1/scaling/status`

查看 Runtime 自动扩缩容评估状态。

**响应**: `200 OK`

```json
{
  "groups": [
    {
      "group_id": "gpu",
      "current_replicas": 2,
      "desired_replicas": 3,
      "average_pressure": 0.91,
      "action": "scale_up",
      "reason": "pressure 0.910 exceeded threshold 0.800",
      "pilot_id": "pilot-12345"
    }
  ]
}
```

**响应**: `200 OK`

```json
{
  "total_calls": 1234,
  "by_rune": [
    {
      "rune_name": "translate",
      "count": 800,
      "avg_latency_ms": 120.5,
      "success_rate": 0.995,
      "p95_latency_ms": 350
    }
  ]
}
```

### `GET /api/v1/logs`

查看调用日志。

**查询参数**:
- `rune` -- 按 Rune 名称过滤（可选）
- `limit` -- 返回条数，默认 50，最大 500（可选）

**响应**: `200 OK`

```json
{
  "logs": [
    {
      "id": 1,
      "request_id": "r-18e8f3a1b-0",
      "rune_name": "translate",
      "mode": "sync",
      "caster_id": null,
      "latency_ms": 120,
      "status_code": 200,
      "input_size": 45,
      "output_size": 38,
      "timestamp": "2026-03-23T10:00:00Z"
    }
  ]
}
```

---

## API Key 管理

> **权限要求**: 以下所有 Key 管理端点（创建、列出、吊销）均需要 **admin key** 认证。使用非 admin 类型的 key 会返回 `403 FORBIDDEN`。开发模式下跳过权限检查。

### `POST /api/v1/keys`

创建新的 API Key。**需要 admin key。**

**请求体**:

```json
{
  "key_type": "gate",
  "label": "my-app"
}
```

- `key_type` -- `gate`（HTTP 调用）、`caster`（gRPC 连接）或 `admin`（管理操作）
- `label` -- 人类可读标签

**响应**: `201 Created`

```json
{
  "raw_key": "rk_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "key": {
    "id": 1,
    "key_type": "gate",
    "label": "my-app",
    "key_prefix": "rk_xxxx",
    "created_at": "2026-03-23T10:00:00Z",
    "revoked_at": null
  }
}
```

**注**: `raw_key` 只在创建时返回一次，之后无法再获取。

### `GET /api/v1/keys`

列出所有 API Key（不包含原始 key 值）。**需要 admin key。**

**响应**: `200 OK`

```json
{
  "keys": [
    {
      "id": 1,
      "key_type": "gate",
      "label": "my-app",
      "key_prefix": "rk_xxxx",
      "created_at": "2026-03-23T10:00:00Z",
      "revoked_at": null
    }
  ]
}
```

### `DELETE /api/v1/keys/{id}`

吊销 API Key。吊销后该 Key 立即失效。**需要 admin key。**

**路径参数**:
- `id` -- Key ID（数字）

**响应**: `200 OK`

```json
{"status": "revoked", "id": 1}
```

---

## OpenAPI

### `GET /api/v1/openapi.json`

返回基于当前在线 Rune 的 schema 自动生成的 OpenAPI 3.0 文档。

**响应**: `200 OK`，Content-Type: `application/json`

文档包含所有声明了 `gate.path` 的 Rune，使用其 `input_schema`、`output_schema`、`description` 字段生成。

---

## 文件下载

### `GET /api/v1/files/{id}`

下载通过 multipart 上传的文件。

**路径参数**:
- `id` -- 文件 ID（由上传响应中的 `file_id` 字段提供）

**响应**:
- `200 OK` -- Content-Type 为文件原始 MIME 类型，Content-Disposition 为 attachment
- `404 NOT_FOUND` -- 文件不存在

---

## Flow API

### `POST /api/v1/flows`

注册新的 DAG Flow。

**请求体**:

```json
{
  "name": "translate-pipeline",
  "steps": [
    {
      "name": "detect",
      "rune": "detect-language",
      "depends_on": []
    },
    {
      "name": "translate",
      "rune": "translate",
      "depends_on": ["detect"],
      "condition": "steps.detect.output.lang != \"en\""
    },
    {
      "name": "summarize",
      "rune": "summarize",
      "depends_on": ["translate"],
      "input_mapping": {
        "text": "translate.output.translated"
      }
    }
  ],
  "gate_path": "/pipeline"
}
```

**响应**:
- `201 Created` -- 返回 flow 定义
- `400 DAG_ERROR` -- DAG 验证失败（环、依赖不存在、重复 step 名等）
- `409 CONFLICT` -- 同名 Flow 已存在
- `422 INVALID_INPUT` -- 无效 JSON

### `GET /api/v1/flows`

列出所有已注册 Flow。

**响应**: `200 OK`

```json
[
  {
    "name": "translate-pipeline",
    "steps_count": 3,
    "gate_path": "/pipeline"
  }
]
```

### `GET /api/v1/flows/{name}`

获取 Flow 详情。

**响应**:
- `200 OK` -- 返回完整 flow 定义
- `404 FLOW_NOT_FOUND`

### `DELETE /api/v1/flows/{name}`

删除 Flow。

**响应**:
- `204 No Content`
- `404 FLOW_NOT_FOUND`

### `POST /api/v1/flows/{name}/run`

执行 Flow。支持与 Rune 调用相同的三种模式。

**查询参数**:
- `stream=true` -- SSE 流式模式
- `async=true` -- 异步模式

**请求体**: Flow 的输入 JSON

#### Sync 模式

**响应**: `200 OK`

```json
{
  "output": {"translated": "你好", "summary": "greeting"},
  "steps_executed": 3
}
```

#### Stream 模式

SSE 事件：
```
event: result
data: {"output": {...}, "steps_executed": 3}

event: done
data: [DONE]
```

#### Async 模式

**响应**: `202 Accepted`

```json
{
  "task_id": "r-18e8f3a1b-1",
  "flow": "translate-pipeline",
  "status": "running"
}
```

---

## 错误格式

所有错误响应统一格式：

```json
{
  "error": {
    "code": "NOT_FOUND",
    "message": "rune 'xyz' not found",
    "request_id": "req-abc123"
  }
}
```

`request_id` 在可用时返回（来自请求头 `X-Request-Id` 或自动生成），便于全链路问题追踪。

### Rune 调用错误码

| 状态码 | 错误码 | 说明 |
|--------|--------|------|
| 400 | BAD_REQUEST | 请求体读取失败或参数错误 |
| 400 | INVALID_INPUT | 无效输入格式 |
| 400 | STREAM_NOT_SUPPORTED | Rune 不支持流式调用 |
| 401 | UNAUTHORIZED | 认证失败（Key 无效或缺失） |
| 403 | FORBIDDEN | 权限不足（Key 类型不匹配等） |
| 404 | NOT_FOUND | Rune/Task/File 不存在 |
| 409 | CONFLICT | 资源冲突（任务已完成等） |
| 422 | VALIDATION_FAILED | 输入 schema 校验失败 |
| 429 | RATE_LIMITED | 请求频率超限 |
| 499 | CANCELLED | 请求被取消（客户端断连） |
| 500 | INTERNAL | 内部错误 |
| 500 | EXECUTION_FAILED | Caster 执行失败（返回错误结果） |
| 500 | OUTPUT_VALIDATION_FAILED | 输出 schema 校验失败 |
| 503 | UNAVAILABLE | Caster 并发已满 |
| 503 | NO_MATCHING_CASTER | 标签路由无匹配 Caster |
| 503 | CIRCUIT_OPEN | 目标 Caster 断路器处于 Open 状态 |
| 504 | TIMEOUT | 请求超时（Caster 执行超时） |

### Flow API 错误码

| 状态码 | 错误码 | 说明 |
|--------|--------|------|
| 400 | DAG_ERROR | DAG 拓扑错误（循环依赖、缺失步骤等） |
| 401 | STEP_UNAUTHORIZED | Flow 步骤认证失败 |
| 403 | STEP_FORBIDDEN | Flow 步骤权限不足 |
| 404 | FLOW_NOT_FOUND | Flow 不存在 |
| 409 | CONFLICT | Flow 名重复 |
| 429 | STEP_RATE_LIMITED | Flow 步骤触发频率限制 |
| 500 | STEP_FAILED | Flow 步骤执行失败（通用） |
| 500 | NO_TERMINAL_STEP | Flow 无终止步骤（内部错误） |
| 500 | SERIALIZATION_FAILED | Flow 步骤间数据序列化失败 |
| 503 | STEP_UNAVAILABLE | Flow 步骤目标 Caster 不可用 |
| 503 | STEP_CIRCUIT_OPEN | Flow 步骤目标断路器 Open |
| 504 | STEP_TIMEOUT | Flow 步骤执行超时 |
