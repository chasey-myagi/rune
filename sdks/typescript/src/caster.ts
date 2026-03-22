import * as path from 'path';
import * as crypto from 'crypto';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import type { CasterOptions, RuneConfig, RuneContext, ReconnectOptions, FileAttachment } from './types.js';
import type { RuneHandler, RuneHandlerWithFiles, StreamRuneHandler, StreamRuneHandlerWithFiles } from './handler.js';
import { StreamSender } from './stream.js';

/** Default gRPC endpoint */
const DEFAULT_RUNTIME = 'localhost:50070';
/** Default heartbeat interval in ms */
const DEFAULT_HEARTBEAT_MS = 10000;
/** Default max concurrent requests */
const DEFAULT_MAX_CONCURRENT = 10;

/** Default reconnection settings */
const DEFAULT_RECONNECT: Required<ReconnectOptions> = {
  enabled: true,
  initialDelayMs: 1000,
  maxDelayMs: 30000,
  backoffMultiplier: 2,
};

interface RegisteredRune {
  config: RuneConfig;
  handler: RuneHandler | RuneHandlerWithFiles | StreamRuneHandler | StreamRuneHandlerWithFiles;
  isStream: boolean;
  acceptsFiles: boolean;
}

// ---------------------------------------------------------------------------
// Proto loading helpers
// ---------------------------------------------------------------------------

const PROTO_PATH = path.resolve(
  // __dirname points to src/ (dev) or dist/ (built) — go up to package root
  // then navigate to the proto file relative to the repo root
  // The SDK lives at: sdks/typescript/
  // The proto lives at: proto/rune/wire/v1/rune.proto
  path.dirname(new URL(import.meta.url).pathname),
  '../../../../proto/rune/wire/v1/rune.proto',
);

function loadProto(): {
  RuneServiceClient: grpc.ServiceClientConstructor;
  SessionMessage: Record<string, unknown>;
} {
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDefinition) as Record<string, any>;
  const v1 = proto.rune.wire.v1;
  return {
    RuneServiceClient: v1.RuneService as grpc.ServiceClientConstructor,
    SessionMessage: v1.SessionMessage,
  };
}

// ---------------------------------------------------------------------------
// Caster
// ---------------------------------------------------------------------------

/**
 * Caster connects to a Rune Runtime and registers Rune handlers.
 *
 * @example
 * ```typescript
 * const caster = new Caster({ key: "rk_xxx" });
 *
 * caster.rune({ name: "greet" }, async (ctx, input) => {
 *   return { message: `Hello, ${(input as any).name}!` };
 * });
 *
 * await caster.run();
 * ```
 */
export class Caster {
  readonly runtime: string;
  readonly key: string;
  readonly casterId: string;
  readonly heartbeatIntervalMs: number;
  readonly maxConcurrent: number;
  readonly labels: Record<string, string>;
  readonly reconnect: Required<ReconnectOptions>;

  private _runes: Map<string, RegisteredRune> = new Map();
  private _stopped = false;
  private _abortControllers: Map<string, AbortController> = new Map();

  constructor(options: CasterOptions) {
    this.runtime = options.runtime ?? DEFAULT_RUNTIME;
    this.key = options.key;
    this.casterId = options.casterId ?? crypto.randomUUID();
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS;
    this.maxConcurrent = options.maxConcurrent ?? DEFAULT_MAX_CONCURRENT;
    this.labels = options.labels ?? {};
    this.reconnect = {
      ...DEFAULT_RECONNECT,
      ...options.reconnect,
    };
  }

  /**
   * Register a unary Rune handler.
   * @throws Error if a Rune with the same name is already registered
   */
  rune(config: RuneConfig, handler: RuneHandler | RuneHandlerWithFiles): void {
    if (this._runes.has(config.name)) {
      throw new Error(`Rune "${config.name}" is already registered`);
    }
    // Unary: (ctx, input) = 2 params, (ctx, input, files) = 3 params
    const acceptsFiles = handler.length >= 3;
    this._runes.set(config.name, { config, handler, isStream: false, acceptsFiles });
  }

  /**
   * Register a streaming Rune handler.
   * @throws Error if a Rune with the same name is already registered
   */
  streamRune(config: RuneConfig, handler: StreamRuneHandler | StreamRuneHandlerWithFiles): void {
    if (this._runes.has(config.name)) {
      throw new Error(`Rune "${config.name}" is already registered`);
    }
    // Stream: (ctx, input, stream) = 3 params, (ctx, input, files, stream) = 4 params
    const acceptsFiles = handler.length >= 4;
    this._runes.set(config.name, {
      config: { ...config, supportsStream: true },
      handler,
      isStream: true,
      acceptsFiles,
    });
  }

  /**
   * Returns the number of registered Runes.
   */
  get runeCount(): number {
    return this._runes.size;
  }

  /**
   * Returns the config of a registered Rune by name, or undefined.
   */
  getRuneConfig(name: string): RuneConfig | undefined {
    return this._runes.get(name)?.config;
  }

  /**
   * Check if a rune is registered as a stream handler.
   */
  isStreamRune(name: string): boolean {
    return this._runes.get(name)?.isStream ?? false;
  }

  /**
   * Check if a rune handler accepts file attachments.
   */
  runeAcceptsFiles(name: string): boolean {
    return this._runes.get(name)?.acceptsFiles ?? false;
  }

  /**
   * Stop the Caster. Current session will end and no reconnection will happen.
   */
  stop(): void {
    this._stopped = true;
  }

  // -----------------------------------------------------------------------
  // Run — public entry point
  // -----------------------------------------------------------------------

  /**
   * Start the Caster: connect to Runtime, send Attach, and begin handling requests.
   * Returns a Promise that resolves when the Caster is shut down.
   *
   * Auto-reconnects with exponential backoff on errors (unless reconnect.enabled is false).
   */
  async run(): Promise<void> {
    this._stopped = false;
    let delay = this.reconnect.initialDelayMs;

    while (!this._stopped) {
      try {
        await this._connectAndRun();
        // Session ended normally — don't reconnect
        break;
      } catch (err) {
        if (!this.reconnect.enabled || this._stopped) {
          throw err;
        }
        // Exponential backoff
        await this._sleep(delay);
        delay = Math.min(delay * this.reconnect.backoffMultiplier, this.reconnect.maxDelayMs);
      }
    }
  }

  // -----------------------------------------------------------------------
  // gRPC session
  // -----------------------------------------------------------------------

  private async _connectAndRun(): Promise<void> {
    const { RuneServiceClient } = loadProto();

    // Establish gRPC channel with API key metadata
    const credentials = grpc.credentials.createInsecure();
    const client = new RuneServiceClient(this.runtime, credentials);

    // Open bidirectional stream
    const stream = (client as any).Session() as grpc.ClientDuplexStream<any, any>;

    // Build and send CasterAttach
    const declarations = this._buildDeclarations();
    stream.write({
      attach: {
        caster_id: this.casterId,
        runes: declarations,
        labels: this.labels,
        max_concurrent: this.maxConcurrent,
      },
    });

    // Heartbeat timer
    const heartbeatTimer = setInterval(() => {
      if (!this._stopped) {
        stream.write({
          heartbeat: {
            timestamp_ms: String(Date.now()),
          },
        });
      }
    }, this.heartbeatIntervalMs);

    return new Promise<void>((resolve, reject) => {
      stream.on('data', (msg: any) => {
        const payload = msg.payload;

        if (payload === 'attach_ack') {
          const ack = msg.attach_ack;
          if (!ack.accepted) {
            clearInterval(heartbeatTimer);
            stream.end();
            reject(new Error(`Attach rejected: ${ack.reason}`));
          }
          // else: attached successfully, continue
        } else if (payload === 'execute') {
          this._handleExecute(msg.execute, stream);
        } else if (payload === 'cancel') {
          this._handleCancel(msg.cancel);
        } else if (payload === 'heartbeat') {
          // Server heartbeat — acknowledged silently
        }
      });

      stream.on('error', (err: Error) => {
        clearInterval(heartbeatTimer);
        reject(err);
      });

      stream.on('end', () => {
        clearInterval(heartbeatTimer);
        resolve();
      });
    });
  }

  // -----------------------------------------------------------------------
  // Declaration builder
  // -----------------------------------------------------------------------

  private _buildDeclarations(): Array<Record<string, unknown>> {
    const declarations: Array<Record<string, unknown>> = [];
    for (const registered of this._runes.values()) {
      const cfg = registered.config;
      const decl: Record<string, unknown> = {
        name: cfg.name,
        version: cfg.version ?? '0.0.0',
        description: cfg.description ?? '',
        supports_stream: cfg.supportsStream ?? false,
        priority: cfg.priority ?? 0,
      };

      if (cfg.inputSchema) {
        decl.input_schema = JSON.stringify(cfg.inputSchema);
      }
      if (cfg.outputSchema) {
        decl.output_schema = JSON.stringify(cfg.outputSchema);
      }
      if (cfg.gate) {
        decl.gate = {
          path: cfg.gate.path,
          method: cfg.gate.method ?? 'POST',
        };
      }

      declarations.push(decl);
    }
    return declarations;
  }

  // -----------------------------------------------------------------------
  // Execute dispatch
  // -----------------------------------------------------------------------

  private _handleExecute(req: any, stream: grpc.ClientDuplexStream<any, any>): void {
    const registered = this._runes.get(req.rune_name);

    if (!registered) {
      stream.write({
        result: {
          request_id: req.request_id,
          status: 'STATUS_FAILED',
          error: {
            code: 'NOT_FOUND',
            message: `rune '${req.rune_name}' not found`,
          },
        },
      });
      return;
    }

    // AbortController for cancellation
    const ac = new AbortController();
    this._abortControllers.set(req.request_id, ac);

    // Build context
    const attachments: FileAttachment[] = (req.attachments ?? []).map((a: any) => ({
      filename: a.filename,
      data: Buffer.from(a.data),
      mimeType: a.mime_type,
    }));

    const ctx: RuneContext = {
      runeName: req.rune_name,
      requestId: req.request_id,
      context: req.context ?? {},
      signal: ac.signal,
      ...(attachments.length > 0 ? { attachments } : {}),
    };

    // Parse input
    let input: unknown;
    try {
      const inputBytes = req.input as Buffer;
      if (inputBytes && inputBytes.length > 0) {
        input = JSON.parse(inputBytes.toString('utf-8'));
      }
    } catch {
      input = req.input;
    }

    if (registered.isStream) {
      this._executeStream(registered, ctx, req, stream, input).finally(() => {
        this._abortControllers.delete(req.request_id);
      });
    } else {
      this._executeOnce(registered, ctx, req, stream, input).finally(() => {
        this._abortControllers.delete(req.request_id);
      });
    }
  }

  private async _executeOnce(
    registered: RegisteredRune,
    ctx: RuneContext,
    req: any,
    stream: grpc.ClientDuplexStream<any, any>,
    input: unknown,
  ): Promise<void> {
    try {
      const handler = registered.handler as RuneHandler;
      const output = await handler(ctx, input);

      // If cancelled during execution, discard result
      if (ctx.signal.aborted) {
        return;
      }

      // Serialize output
      const outputBuf =
        output instanceof Buffer
          ? output
          : typeof output === 'string'
            ? Buffer.from(output)
            : Buffer.from(JSON.stringify(output));

      stream.write({
        result: {
          request_id: req.request_id,
          status: 'STATUS_COMPLETED',
          output: outputBuf,
        },
      });
    } catch (err: any) {
      stream.write({
        result: {
          request_id: req.request_id,
          status: 'STATUS_FAILED',
          error: {
            code: 'EXECUTION_FAILED',
            message: String(err?.message ?? err),
          },
        },
      });
    }
  }

  private async _executeStream(
    registered: RegisteredRune,
    ctx: RuneContext,
    req: any,
    grpcStream: grpc.ClientDuplexStream<any, any>,
    input: unknown,
  ): Promise<void> {
    const sender = new StreamSender();

    // Attach the real send function
    sender._attach((data: Buffer) => {
      grpcStream.write({
        stream_event: {
          request_id: req.request_id,
          data,
        },
      });
    });

    try {
      const handler = registered.handler as StreamRuneHandler;
      await handler(ctx, input, sender);

      // Send StreamEnd on completion
      grpcStream.write({
        stream_end: {
          request_id: req.request_id,
          status: 'STATUS_COMPLETED',
        },
      });
    } catch (err: any) {
      grpcStream.write({
        stream_end: {
          request_id: req.request_id,
          status: 'STATUS_FAILED',
          error: {
            code: 'EXECUTION_FAILED',
            message: String(err?.message ?? err),
          },
        },
      });
    }
  }

  // -----------------------------------------------------------------------
  // Cancel
  // -----------------------------------------------------------------------

  private _handleCancel(cancel: any): void {
    const ac = this._abortControllers.get(cancel.request_id);
    if (ac) {
      ac.abort(cancel.reason ?? 'cancelled');
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private _sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
