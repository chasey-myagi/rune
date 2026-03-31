# Agent Bridge — HTML ↔ Claude Code 双向通信

> 日期：2026-03-31
> 状态：Scope Draft
> 依赖：Rune v0.2.0（无内核改动）

---

## 1. 问题

Claude Code 生成的 HTML 页面和 Claude Code 自身是**单向割裂**的：
- Claude Code 能写 HTML → 浏览器打开 ✅
- 用户在 HTML 里的操作 → 无法传回 Claude Code ❌
- Claude Code 的后续输出 → 无法推送到 HTML ❌

具体场景：
- **LearnKit**：用户在教案 HTML 里划词提问，希望 Claude Code 直接回答并渲染在页面上
- **Superpowers HTML**：Claude Code 生成了交互式设计预览，用户选了方案 B，但得切回终端告诉 Claude Code
- **任何 HTML 工具**：表单提交、按钮点击、拖拽操作——所有用户交互都无法直接到达 Claude Code

## 2. 解决方案

利用已有的 Rune Runtime 作为本地 HTTP 桥，Bridge Caster 做 Claude Code mailbox 文件的读写。

```
浏览器 HTML
  │
  ├─ fetch POST http://localhost:50060/ask ──→ Rune Gate ──→ Bridge Caster
  │                                                              │
  │                                                    写入 Claude Code mailbox
  │                                                    ~/.claude/teams/{team}/inboxes/{agent}.json
  │                                                              │
  │                                              Claude Code 下一个 tool round 读到
  │                                              处理后写回复到约定文件
  │                                                              │
  ← SSE stream ←── Gate ←── Bridge Caster ←── 轮询回复文件 ──────┘
```

### 为什么不需要改 Rune 内核

| 能力 | 现状 |
|------|------|
| HTTP 端点 + SSE 流式 | Gate 已支持 |
| Caster 注册 + stream handler | SDK 已支持 |
| CORS（浏览器跨域） | Gate 已支持 |
| 本地运行 | `rune start --dev` 已支持 |

## 3. 交付物

### 3.1 Bridge Caster（Python）

一个独立的 Caster 脚本，注册一个 stream Rune `ask`（gate: `/ask`）。

**职责：**
1. 收到 HTTP 请求（来自 HTML 的 fetch）
2. 将问题写入 Claude Code 的 teammate mailbox JSON
3. 轮询约定的回复文件（或 mailbox 回复消息）
4. 流式返回回复内容（SSE）
5. 超时处理（30s 无回复则返回超时提示）

**mailbox 协议：**

写入 Claude Code inbox：
```json
{
  "from": "html-bridge",
  "text": "{\"type\":\"question\",\"question\":\"什么是生命周期？\",\"context\":\"Rust 教案第3章\",\"selection\":\"生命周期\"}",
  "timestamp": "2026-03-31T12:00:00Z",
  "read": false,
  "summary": "用户划词提问：生命周期"
}
```

Claude Code 回复方式：写入一个约定路径的文件（如 `/tmp/rune-bridge-reply-{request_id}.json`），Bridge Caster 轮询此文件。

### 3.2 Skill — `interactive-html`

教 Claude Code 两件事：
1. **生成带通信能力的 HTML**——模板里自带 fetch 逻辑
2. **处理 mailbox 中的 HTML 交互消息**——读到后理解并回复

#### Skill 提供的 HTML 交互模板

Claude Code 生成 HTML 时，Skill 指导它注入以下代码：

```html
<!-- 划词提问 -->
<script>
const RUNE_BRIDGE = 'http://localhost:50060';

document.addEventListener('mouseup', async () => {
  const text = window.getSelection().toString().trim();
  if (text.length < 2) return;

  const bubble = showBubble('思考中...', event);

  try {
    const res = await fetch(`${RUNE_BRIDGE}/ask`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        question: text,
        context: document.title,
        page_content: getVisibleContent()
      })
    });

    // 流式读取
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let answer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      const chunk = decoder.decode(value);
      // 解析 SSE data: 行
      for (const line of chunk.split('\n')) {
        if (line.startsWith('data: ')) {
          const data = line.slice(6);
          if (data === '[DONE]') break;
          answer += data;
          updateBubble(bubble, answer);
        }
      }
    }
  } catch (e) {
    updateBubble(bubble, '连接失败，请确保 rune start --dev 正在运行');
  }
});
</script>
```

#### Skill 提供的 Claude Code 侧指导

```markdown
当你收到来自 html-bridge 的 mailbox 消息时：
1. 解析 JSON：type=question 表示用户在 HTML 里提了问题
2. question 是用户选中的文本或输入的问题
3. context 是页面标题/上下文
4. 将回答写入 /tmp/rune-bridge-reply-{request_id}.json
5. 回答应该简洁、直接回应问题
```

### 3.3 交互模式扩展

除了划词提问，Skill 可以定义更多交互模式：

| 模式 | HTML 端 | Claude Code 端 |
|------|---------|---------------|
| 划词提问 | mouseup 事件 + 选中文本 | 回答问题 |
| 方案选择 | 按钮点击 + 选项值 | 接收选择，继续工作流 |
| 表单提交 | form submit + 字段数据 | 处理表单数据 |
| 实时反馈 | 评分/点赞/修改建议 | 调整输出 |

所有模式走同一个 `/ask` 端点，通过 `type` 字段区分。

## 4. 不做什么

- 不改 Rune 内核（Gate/Core/Flow）
- 不改 Claude Code 源码
- 不做 WebSocket（SSE 足够，且 Gate 已原生支持）
- 不做认证（本地开发场景，--dev 模式）
- 不做多用户（单用户单 Claude Code 会话）

## 5. 用户体验

```bash
# 1. 启动 Runtime
rune start --dev

# 2. 启动 Bridge Caster（随 Skill 提供）
python bridge_caster.py

# 3. Claude Code 正常工作，生成带交互的 HTML
#    用户在 HTML 里划词提问
#    Claude Code 自动收到并回答
#    回答实时渲染在 HTML 页面上
```

## 6. 决策记录

| # | 问题 | 决策 | 理由 |
|---|------|------|------|
| D1 | 回复通道 | Claude Code 写 `/tmp/rune-bridge-reply-{request_id}.json` | 最简单，一行 Write tool 调用，无需理解 mailbox 格式 |
| D2 | 多 HTML 页面 | page_id 区分，request_id 一对一匹配 | Bridge Caster 不需要关心路由，各页面 fetch 各自独立 |
| D3 | 会话持续性 | 不做重连，超时显示"会话已断开" | 本地开发场景，重启后用户自然重开页面，过度设计 |
| D4 | Bridge Caster 生命周期 | Skill INIT 阶段自动启动 | 检测 `rune list | grep ask`，没有就后台拉起 |
