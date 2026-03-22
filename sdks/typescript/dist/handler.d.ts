import type { RuneContext } from './types.js';
import type { StreamSender } from './stream.js';
/**
 * Handler for a unary (request-response) Rune.
 * Receives context and input, returns the output.
 */
export type RuneHandler = (ctx: RuneContext, input: unknown) => Promise<unknown>;
/**
 * Handler for a streaming Rune.
 * Receives context, input, and a StreamSender for emitting chunks.
 * Should return when streaming is complete; stream.end() is called automatically.
 */
export type StreamRuneHandler = (ctx: RuneContext, input: unknown, stream: StreamSender) => Promise<void>;
