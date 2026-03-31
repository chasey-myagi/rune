# Agent Bridge — HTML ↔ Claude Code 通信

让浏览器 HTML 页面和正在运行的 Claude Code 会话双向通信。

## 原理

```
HTML (fetch /ask) → Rune Gate → Bridge Caster → Claude Code mailbox
HTML (SSE) ← Rune Gate ← Bridge Caster ← 轮询回复文件
```

## 使用

### 1. 启动 Runtime

```bash
rune start --dev
```

### 2. 启动 Bridge Caster

```bash
cd examples/agent-bridge
pip install -r requirements.txt
python bridge_caster.py
```

### 3. 打开演示页面

```bash
open demo.html
```

选中页面上的任何文字，Claude Code 会收到提问并回答。

### 4. Claude Code 侧

在 Claude Code 中，你会在 mailbox 里收到来自 `html-bridge` 的消息。处理后将回答写入消息中指定的 `reply_to` 路径即可。

## 环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `RUNE_ADDR` | `localhost:50070` | Runtime gRPC 地址 |
| `RUNE_BRIDGE_TEAM` | `default` | Claude Code team 名 |
| `RUNE_BRIDGE_AGENT` | `team-lead` | 目标 agent 名 |

## 交互协议

HTML 发送到 `/ask` 的 JSON 格式：

```json
{
  "type": "question",
  "request_id": "abc123",
  "question": "选中的文本",
  "context": "页面标题",
  "page_id": "页面ID"
}
```

Claude Code 回复文件（`/tmp/rune-bridge-reply-{request_id}.json`）：

```json
{
  "answer": "回答内容",
  "request_id": "abc123"
}
```
