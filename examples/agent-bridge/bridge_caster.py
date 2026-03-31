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
