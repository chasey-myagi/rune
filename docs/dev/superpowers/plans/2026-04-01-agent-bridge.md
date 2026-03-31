# Agent Bridge 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 HTML ↔ Claude Code 双向通信，通过 Rune Bridge Caster 中转 Claude Code mailbox。

**Architecture:** Bridge Caster（Python）注册 stream Rune `ask`（gate: `/ask`），收到 HTTP 请求后写入 Claude Code teammate mailbox JSON 文件，轮询回复文件，流式返回。HTML 端通过原生 fetch + SSE 通信，零额外依赖。

**Tech Stack:** Python, rune-framework SDK, asyncio, SSE

**Spec:** `docs/dev/superpowers/specs/2026-03-31-agent-bridge-scope.md`

---

## File Structure

### 新建文件

| 文件 | 职责 |
|------|------|
| `examples/agent-bridge/bridge_caster.py` | Bridge Caster 主文件 |
| `examples/agent-bridge/requirements.txt` | Python 依赖 |
| `examples/agent-bridge/README.md` | 使用说明 |
| `examples/agent-bridge/demo.html` | 划词提问演示页面 |

---

## Task 1: Bridge Caster 核心实现

**Files:**
- Create: `examples/agent-bridge/bridge_caster.py`
- Create: `examples/agent-bridge/requirements.txt`

- [ ] **Step 1: 创建 requirements.txt**

```
rune-framework>=1.1.0
```

- [ ] **Step 2: 实现 Bridge Caster**

```python
#!/usr/bin/env python3
"""
Rune Bridge Caster — HTML ↔ Claude Code 双向通信桥

注册一个 stream Rune "ask"（gate: /ask），收到 HTTP 请求后：
1. 写入 Claude Code 的 teammate mailbox
2. 轮询回复文件
3. 流式返回
"""

import asyncio
import json
import os
import time
import uuid
from pathlib import Path

from rune import Caster

# ── 配置 ─────────────────────────────────────────────────

RUNTIME_ADDR = os.environ.get("RUNE_ADDR", "localhost:50070")
TEAM_NAME = os.environ.get("RUNE_BRIDGE_TEAM", "default")
AGENT_NAME = os.environ.get("RUNE_BRIDGE_AGENT", "team-lead")
REPLY_DIR = Path(os.environ.get("RUNE_BRIDGE_REPLY_DIR", "/tmp"))
POLL_INTERVAL = 0.5  # seconds
REPLY_TIMEOUT = 60   # seconds

caster = Caster(
    RUNTIME_ADDR,
    caster_id="html-bridge",
    max_concurrent=10,
)


# ── Mailbox 读写 ────────────────────────────────────────

def get_inbox_path(agent_name: str, team_name: str) -> Path:
    """Claude Code teammate mailbox 路径"""
    teams_dir = Path.home() / ".claude" / "teams"
    return teams_dir / team_name / "inboxes" / f"{agent_name}.json"


def write_to_mailbox(agent_name: str, team_name: str, message: dict) -> None:
    """写入一条消息到 Claude Code 的 mailbox"""
    inbox_path = get_inbox_path(agent_name, team_name)
    inbox_path.parent.mkdir(parents=True, exist_ok=True)

    # 读取现有消息
    messages = []
    if inbox_path.exists():
        try:
            messages = json.loads(inbox_path.read_text())
        except (json.JSONDecodeError, OSError):
            messages = []

    # 追加新消息
    messages.append(message)
    inbox_path.write_text(json.dumps(messages, ensure_ascii=False, indent=2))


def get_reply_path(request_id: str) -> Path:
    """回复文件路径"""
    return REPLY_DIR / f"rune-bridge-reply-{request_id}.json"


async def wait_for_reply(request_id: str, timeout: float = REPLY_TIMEOUT) -> dict | None:
    """轮询等待回复文件"""
    reply_path = get_reply_path(request_id)
    deadline = time.time() + timeout

    while time.time() < deadline:
        if reply_path.exists():
            try:
                reply = json.loads(reply_path.read_text())
                reply_path.unlink(missing_ok=True)  # 读完即删
                return reply
            except (json.JSONDecodeError, OSError):
                pass
        await asyncio.sleep(POLL_INTERVAL)

    return None


# ── Rune Handler ─────────────────────────────────────────

@caster.stream_rune("ask", gate="/ask")
async def ask(ctx, input_bytes, sender):
    """
    处理来自 HTML 的请求：
    1. 解析请求 JSON
    2. 写入 Claude Code mailbox
    3. 等待回复
    4. 流式返回
    """
    try:
        data = json.loads(input_bytes)
    except json.JSONDecodeError:
        await sender.emit(json.dumps({"error": "Invalid JSON"}))
        return

    request_id = data.get("request_id", str(uuid.uuid4())[:8])
    question = data.get("question", "")
    context = data.get("context", "")
    msg_type = data.get("type", "question")
    page_id = data.get("page_id", "unknown")

    # 构造 mailbox 消息
    mailbox_message = {
        "from": "html-bridge",
        "text": json.dumps({
            "type": msg_type,
            "request_id": request_id,
            "question": question,
            "context": context,
            "page_id": page_id,
            "reply_to": str(get_reply_path(request_id)),
        }, ensure_ascii=False),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "read": False,
        "summary": f"HTML {msg_type}: {question[:50]}",
    }

    # 写入 Claude Code mailbox
    write_to_mailbox(AGENT_NAME, TEAM_NAME, mailbox_message)
    await sender.emit(json.dumps({"status": "sent", "request_id": request_id}))

    # 等待回复
    reply = await wait_for_reply(request_id)

    if reply:
        # 流式返回回复内容
        answer = reply.get("answer", "")
        # 按段落分块发送，模拟流式
        chunks = answer.split("\n\n") if "\n\n" in answer else [answer]
        for chunk in chunks:
            if chunk.strip():
                await sender.emit(chunk.strip())
                await asyncio.sleep(0.05)  # 小延迟让前端有流式感
    else:
        await sender.emit(json.dumps({
            "error": "timeout",
            "message": "Claude Code 未在 60 秒内回复。请确保 Claude Code 正在运行且已加入 team。"
        }))


if __name__ == "__main__":
    print(f"Bridge Caster starting...")
    print(f"  Runtime:  {RUNTIME_ADDR}")
    print(f"  Team:     {TEAM_NAME}")
    print(f"  Agent:    {AGENT_NAME}")
    print(f"  Gate:     /ask")
    caster.run()
```

- [ ] **Step 3: 验证语法**

Run: `cd examples/agent-bridge && python3 -c "import ast; ast.parse(open('bridge_caster.py').read()); print('OK')"`
Expected: OK

- [ ] **Step 4: Commit**

```bash
git add examples/agent-bridge/
git commit -m "feat: add Bridge Caster for HTML ↔ Claude Code communication"
```

---

## Task 2: 演示 HTML 页面

**Files:**
- Create: `examples/agent-bridge/demo.html`

- [ ] **Step 1: 创建演示页面**

一个单文件 HTML，展示划词提问功能。包含：
- 一段示例文本（如 Rune 架构说明）
- 划词后弹出气泡显示"思考中..."
- SSE 流式接收回答，实时渲染
- 无外部依赖，纯原生 JS

关键代码片段（完整 HTML 自行补充样式）：

```html
<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="UTF-8">
<title>Rune Agent Bridge Demo</title>
<style>
  /* 页面样式 + 气泡样式 */
  body { font-family: system-ui; max-width: 720px; margin: 40px auto; padding: 0 20px; line-height: 1.8; color: #1a1a1a; }
  .bubble { position: fixed; max-width: 400px; padding: 16px; background: #1a1a1a; color: #e8e8e8; border-radius: 8px; font-size: 14px; line-height: 1.6; box-shadow: 0 4px 20px rgba(0,0,0,0.3); z-index: 9999; white-space: pre-wrap; }
  .bubble .close { position: absolute; top: 4px; right: 8px; cursor: pointer; opacity: 0.5; }
  .bubble .close:hover { opacity: 1; }
  .bubble.loading { opacity: 0.7; }
  .status { position: fixed; bottom: 12px; right: 12px; font-size: 11px; color: #999; }
</style>
</head>
<body>

<h1>Rune Agent Bridge Demo</h1>
<p class="status" id="status">● 连接中...</p>

<article>
  <h2>什么是 Rune？</h2>
  <p>Rune 是一个协议优先的多语言函数执行框架。开发者只需要定义一个函数（Rune），就能自动获得 HTTP API、流式调用、异步任务和 DAG 工作流编排。</p>

  <h2>核心概念</h2>
  <p><strong>Caster</strong> 是远程执行者——一个通过 gRPC 双向流连接到 Runtime 的独立进程。Caster 可以用 Python、TypeScript 或 Rust 编写。每个 Caster 注册一个或多个 Rune（函数），Runtime 的 Relay 负责路由和调度。</p>

  <p><strong>Gate</strong> 是 HTTP 入口层。当 Caster 注册 Rune 时声明一个 gate path（如 <code>/translate</code>），Gate 自动将这个路径映射为 REST API。同一个 Rune 自动获得三种调用模式：sync（同步）、stream（SSE 流式）、async（异步任务）。</p>

  <p><strong>Flow</strong> 是 DAG 工作流引擎。多个 Rune 可以编排成一个有向无环图（Flow），支持条件分支、并行执行和 input mapping。Flow 使用声明式 JSON 定义，通过 <code>rune flow register</code> 注册。</p>

  <p>试试选中上面任何一段文字，看看会发生什么。</p>
</article>

<script>
const BRIDGE = 'http://localhost:50060';
const PAGE_ID = crypto.randomUUID().slice(0, 8);

// 检查连接状态
async function checkConnection() {
  const el = document.getElementById('status');
  try {
    const res = await fetch(`${BRIDGE}/health`);
    if (res.ok) {
      el.textContent = '● Bridge 已连接';
      el.style.color = '#4ade80';
    }
  } catch {
    el.textContent = '● 未连接 — 请运行 rune start --dev && python bridge_caster.py';
    el.style.color = '#f87171';
  }
}
checkConnection();
setInterval(checkConnection, 10000);

// 划词提问
let currentBubble = null;

document.addEventListener('mouseup', async (e) => {
  const text = window.getSelection().toString().trim();
  if (text.length < 2) return;

  // 关闭旧气泡
  if (currentBubble) currentBubble.remove();

  // 创建气泡
  const bubble = document.createElement('div');
  bubble.className = 'bubble loading';
  bubble.innerHTML = '<span class="close" onclick="this.parentElement.remove()">✕</span>\n思考中...';
  bubble.style.left = `${Math.min(e.clientX, window.innerWidth - 420)}px`;
  bubble.style.top = `${e.clientY + 20}px`;
  document.body.appendChild(bubble);
  currentBubble = bubble;

  const requestId = crypto.randomUUID().slice(0, 8);

  try {
    const res = await fetch(`${BRIDGE}/ask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        type: 'question',
        request_id: requestId,
        question: text,
        context: document.title,
        page_id: PAGE_ID
      })
    });

    if (!res.ok) {
      bubble.innerHTML = '<span class="close" onclick="this.parentElement.remove()">✕</span>\n请求失败: ' + res.status;
      bubble.classList.remove('loading');
      return;
    }

    // 读取 SSE 流
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let answer = '';
    let gotFirstChunk = false;

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;

      const chunk = decoder.decode(value, { stream: true });
      for (const line of chunk.split('\n')) {
        if (!line.startsWith('data: ')) continue;
        const data = line.slice(6);
        if (data === '[DONE]') break;

        try {
          const parsed = JSON.parse(data);
          if (parsed.error) {
            answer = parsed.message || parsed.error;
            break;
          }
          if (parsed.status === 'sent') continue; // 发送确认，跳过
        } catch {
          // 不是 JSON，就是纯文本回答
          if (!gotFirstChunk) {
            answer = '';
            gotFirstChunk = true;
          }
          answer += data + '\n';
        }

        bubble.innerHTML = '<span class="close" onclick="this.parentElement.remove()">✕</span>\n' + answer.trim();
        bubble.classList.remove('loading');
      }
    }

    if (!gotFirstChunk) {
      bubble.innerHTML = '<span class="close" onclick="this.parentElement.remove()">✕</span>\n' + (answer || '无回复');
    }
    bubble.classList.remove('loading');

  } catch (err) {
    bubble.innerHTML = '<span class="close" onclick="this.parentElement.remove()">✕</span>\n连接失败: 请确保 rune start --dev 和 bridge_caster.py 正在运行';
    bubble.classList.remove('loading');
  }
});

// ESC 关闭气泡
document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape' && currentBubble) {
    currentBubble.remove();
    currentBubble = null;
  }
});
</script>

</body>
</html>
```

- [ ] **Step 2: Commit**

```bash
git add examples/agent-bridge/demo.html
git commit -m "feat: add demo HTML page with text selection → ask interaction"
```

---

## Task 3: README 使用说明

**Files:**
- Create: `examples/agent-bridge/README.md`

- [ ] **Step 1: 创建 README**

```markdown
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
```

- [ ] **Step 2: Commit**

```bash
git add examples/agent-bridge/README.md
git commit -m "docs: add Agent Bridge usage guide"
```

---

## Task 4: 端到端验证

- [ ] **Step 1: 验证文件结构**

```bash
ls -la examples/agent-bridge/
# 应有: bridge_caster.py, requirements.txt, demo.html, README.md
```

- [ ] **Step 2: 验证 Python 语法**

```bash
python3 -c "import ast; ast.parse(open('examples/agent-bridge/bridge_caster.py').read()); print('OK')"
```

- [ ] **Step 3: 验证 HTML 语法**

```bash
# 用浏览器打开确认能渲染
open examples/agent-bridge/demo.html
```

- [ ] **Step 4: Final commit**

```bash
git add -A examples/agent-bridge/
git commit -m "feat: Agent Bridge — HTML ↔ Claude Code via Rune Bridge Caster"
```

---

## Summary

| Task | 内容 |
|------|------|
| 1 | Bridge Caster Python 实现（mailbox 读写 + 轮询 + stream handler） |
| 2 | 演示 HTML（划词提问 + SSE 流式接收 + 气泡渲染） |
| 3 | README 使用说明 |
| 4 | 端到端验证 |

完成后的能力：
- 用户在浏览器 HTML 里划词 → fetch POST /ask → Bridge Caster 写 mailbox → Claude Code 读到
- Claude Code 写回复文件 → Bridge Caster 轮询读取 → SSE 流式返回 → HTML 渲染气泡
