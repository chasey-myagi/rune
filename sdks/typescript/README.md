# Rune TypeScript SDK

Official TypeScript Caster SDK for the Rune framework.

## 安装

```bash
npm install @rune-sdk/caster
```

## 快速开始

```typescript
import { Caster } from '@rune-sdk/caster';

const caster = new Caster({ key: 'rk_xxx' });

caster.rune({ name: 'echo', gate: { path: '/echo' } }, async (ctx, input) => {
  return input;
});

await caster.run();
```

启动后 Runtime 自动暴露：
```
POST /echo              -- sync
POST /echo?stream=true  -- SSE streaming
POST /echo?async=true   -- async task
```

## Caster 配置

```typescript
const caster = new Caster({
  runtime: 'localhost:50070',      // Runtime gRPC 地址（默认）
  key: 'rk_xxx',                   // API Key（必填）
  casterId: 'my-ts-caster',       // Caster ID（默认自动生成 UUID）
  maxConcurrent: 10,               // 最大并发处理数（默认 10）
  labels: { env: 'prod' },        // Caster 标签（用于标签路由）
  heartbeatIntervalMs: 10000,     // 心跳间隔毫秒（默认 10000）
  reconnect: {                     // 重连配置
    enabled: true,                 // 是否自动重连（默认 true）
    initialDelayMs: 1000,          // 初始延迟（默认 1000）
    maxDelayMs: 30000,             // 最大延迟（默认 30000）
    backoffMultiplier: 2,          // 退避倍数（默认 2）
  },
});
```

## 注册 Rune

### Unary Handler

```typescript
caster.rune(
  {
    name: 'translate',
    gate: { path: '/translate', method: 'POST' },
    version: '1.0.0',
    description: 'Translate text',
    priority: 10,
    inputSchema: {
      type: 'object',
      properties: {
        text: { type: 'string' },
        lang: { type: 'string' },
      },
      required: ['text', 'lang'],
    },
    outputSchema: {
      type: 'object',
      properties: {
        translated: { type: 'string' },
      },
    },
  },
  async (ctx, input) => {
    const { text, lang } = input as { text: string; lang: string };
    return { translated: doTranslate(text, lang) };
  },
);
```

返回值可以是 `object`（自动 JSON 序列化）、`string`、`Buffer` 或 `null`。

### Streaming Handler

```typescript
caster.streamRune(
  { name: 'generate', gate: { path: '/generate' } },
  async (ctx, input, stream) => {
    const { prompt } = input as { prompt: string };
    for (const token of model.stream(prompt)) {
      stream.emit(Buffer.from(token));
    }
  },
);
```

### File Handler

Unary handler 的第三个参数接收文件附件：

```typescript
caster.rune(
  { name: 'process-doc', gate: { path: '/process' } },
  async (ctx, input, files) => {
    // files: FileAttachment[]
    for (const f of files) {
      console.log(f.filename, f.mimeType, f.data.length);
    }
    return { processed: files.length };
  },
);
```

`FileAttachment` 字段：
- `filename: string` -- 文件名
- `data: Buffer` -- 文件内容
- `mimeType: string` -- MIME 类型

## Context

每次调用都会传入 `RuneContext`：

```typescript
caster.rune({ name: 'example' }, async (ctx, input) => {
  console.log(ctx.runeName);     // "example"
  console.log(ctx.requestId);    // "r-18e8f3a1b-0"
  console.log(ctx.context);      // {} (额外的 key-value 上下文)
  console.log(ctx.signal);       // AbortSignal（取消时触发）
  console.log(ctx.attachments);  // FileAttachment[] | undefined
  return input;
});
```

`ctx.signal` 是标准的 `AbortSignal`，可用于检测取消：

```typescript
caster.rune({ name: 'long-task' }, async (ctx, input) => {
  for (let i = 0; i < 1000; i++) {
    if (ctx.signal.aborted) {
      throw new Error('cancelled');
    }
    await doWork(i);
  }
  return { done: true };
});
```

## 运行与停止

```typescript
// 异步运行（自动重连）
await caster.run();

// 停止
caster.stop();
```

## 类型导出

SDK 导出以下类型：

```typescript
import {
  Caster,
  StreamSender,
  type CasterOptions,
  type RuneConfig,
  type GateConfig,
  type RuneContext,
  type FileAttachment,
  type ReconnectOptions,
  type RuneHandler,
  type RuneHandlerWithFiles,
  type StreamRuneHandler,
  type StreamRuneHandlerWithFiles,
} from '@rune-sdk/caster';
```

## 开发

```bash
cd sdks/typescript
npm install
npm run build          # TypeScript 编译
npm test               # 运行测试
```
