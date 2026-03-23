/**
 * Example TypeScript Caster — registers runes and connects to Rune runtime.
 *
 * Usage:
 *   1. Start the Rune server: cargo run -p rune-server
 *   2. npm install && npx tsx main.ts
 */

import { Caster } from "@rune-sdk/caster";
import type { RuneContext, StreamSender } from "@rune-sdk/caster";

const caster = new Caster({
  key: process.env.RUNE_KEY ?? "dev-key",
  runtime: process.env.RUNE_ADDR ?? "localhost:50070",
  casterId: "example-ts-caster",
  maxConcurrent: 10,
  labels: { lang: "typescript", env: "dev" },
});

// ---------------------------------------------------------------------------
// 1. Echo rune (unary) — simplest possible handler
// ---------------------------------------------------------------------------

caster.rune(
  {
    name: "ts-echo",
    version: "1.0.0",
    description: "Echo input back as-is",
    gate: { path: "/ts/echo" },
  },
  async (_ctx: RuneContext, input: unknown): Promise<unknown> => {
    return input;
  },
);

// ---------------------------------------------------------------------------
// 2. Chat rune (stream) — emits tokens one by one
// ---------------------------------------------------------------------------

caster.streamRune(
  {
    name: "ts-chat",
    version: "1.0.0",
    description: "Streaming chat — emits tokens with delay",
    gate: { path: "/ts/chat" },
  },
  async (_ctx: RuneContext, input: unknown, stream: StreamSender): Promise<void> => {
    const message = (input as Record<string, unknown>)?.message ?? "Hello Rune!";
    const tokens = String(message).split(" ");

    for (const token of tokens) {
      await stream.emit({ token, timestamp: Date.now() });
      // Simulate LLM token-by-token generation delay
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  },
);

// ---------------------------------------------------------------------------
// 3. Summarize rune — with input/output schema declaration
// ---------------------------------------------------------------------------

caster.rune(
  {
    name: "ts-summarize",
    version: "1.0.0",
    description: "Summarize text (mock)",
    gate: { path: "/ts/summarize" },
    inputSchema: {
      type: "object",
      properties: {
        text: { type: "string", minLength: 1 },
        maxLength: { type: "number", minimum: 10, default: 100 },
      },
      required: ["text"],
    },
    outputSchema: {
      type: "object",
      properties: {
        summary: { type: "string" },
        originalLength: { type: "number" },
      },
    },
    priority: 5,
  },
  async (_ctx: RuneContext, input: unknown): Promise<unknown> => {
    const data = input as Record<string, unknown>;
    const text = String(data.text ?? "");
    const maxLen = Number(data.maxLength ?? 100);
    const summary = text.length > maxLen ? text.slice(0, maxLen) + "..." : text;

    return {
      summary,
      originalLength: text.length,
    };
  },
);

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

console.log("Starting TypeScript Caster — connecting to", caster.runtime);
console.log("Registered runes: ts-echo, ts-chat, ts-summarize");

caster.run().catch((err) => {
  console.error("Caster error:", err);
  process.exit(1);
});
