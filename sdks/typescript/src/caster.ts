import * as path from 'path';
import * as crypto from 'crypto';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import type {
  CasterOptions,
  RuneConfig,
  RuneContext,
  ReconnectOptions,
  FileAttachment,
  ScalePolicy,
  LoadReport,
} from './types.js';
import type { RuneHandler, RuneHandlerWithFiles, StreamRuneHandler, StreamRuneHandlerWithFiles } from './handler.js';
import { PilotClient } from './pilot-client.js';
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

// Proto file is bundled inside the SDK package at proto/rune.proto.
// Source of truth: repo root proto/rune/wire/v1/rune.proto — copy when proto changes.
const PROTO_PATH = path.resolve(
  path.dirname(new URL(import.meta.url).pathname),
  '../proto/rune.proto',
);

// Module-level cache for loaded proto (NF-17: avoid re-parsing on every reconnect)
let _protoCache: {
  RuneServiceClient: grpc.ServiceClientConstructor;
  SessionMessage: Record<string, unknown>;
} | null = null;

function loadProto(): {
  RuneServiceClient: grpc.ServiceClientConstructor;
  SessionMessage: Record<string, unknown>;
} {
  if (_protoCache) {
    return _protoCache;
  }
  const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });
  const proto = grpc.loadPackageDefinition(packageDefinition) as Record<string, any>;
  const v1 = proto.rune.wire.v1;
  _protoCache = {
    RuneServiceClient: v1.RuneService as grpc.ServiceClientConstructor,
    SessionMessage: v1.SessionMessage,
  };
  return _protoCache;
}

/**
 * @internal Exposed for testing — returns the cached proto reference.
 */
export function _getProtoCache() {
  return _protoCache;
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
  readonly scalePolicy?: ScalePolicy;
  readonly loadReport?: LoadReport;
  readonly reconnect: Required<ReconnectOptions>;

  private _runes: Map<string, RegisteredRune> = new Map();
  private _stopped = false;
  private _abortControllers: Map<string, AbortController> = new Map();
  private _activeStream: grpc.ClientDuplexStream<any, any> | null = null;
  private _activeRequests = 0;

  constructor(options: CasterOptions) {
    this.runtime = options.runtime ?? DEFAULT_RUNTIME;
    this.key = options.key;
    this.casterId = options.casterId ?? crypto.randomUUID();
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_MS;
    this.maxConcurrent = options.maxConcurrent ?? DEFAULT_MAX_CONCURRENT;
    this.labels = options.labels ?? {};
    this.scalePolicy = options.scalePolicy;
    this.loadReport = options.loadReport;
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
    if (this._activeStream) {
      try {
        this._activeStream.end();
      } catch {
        // ignore errors on close
      }
      this._activeStream = null;
    }
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
    const pilotClient = this.scalePolicy
      ? await PilotClient.ensure(this.runtime, this.key)
      : null;
    if (pilotClient && this.scalePolicy) {
      await pilotClient.register(this.casterId, this.scalePolicy);
    }
    const pilotId = pilotClient?.pilotId;

    try {
      while (!this._stopped) {
        try {
          await this._connectAndRun(pilotId);
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
    } finally {
      if (pilotClient) {
        await pilotClient.deregister(this.casterId).catch(() => {});
      }
    }
  }

  // -----------------------------------------------------------------------
  // gRPC session
  // -----------------------------------------------------------------------

  private async _connectAndRun(pilotId?: string): Promise<void> {
    const { RuneServiceClient } = loadProto();

    // Establish gRPC channel with API key metadata
    const credentials = grpc.credentials.createInsecure();
    const client = new RuneServiceClient(this.runtime, credentials);

    // Open bidirectional stream
    const stream = (client as any).Session() as grpc.ClientDuplexStream<any, any>;
    this._activeStream = stream;

    // Build and send CasterAttach
    stream.write(this._buildAttachMessage(pilotId));

    // Heartbeat timer
    const heartbeatTimer = setInterval(() => {
      if (!this._stopped) {
        stream.write({
          heartbeat: {
            timestamp_ms: String(Date.now()),
          },
        });
        if (this.scalePolicy) {
          stream.write(this._buildHealthReport());
        }
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
          if (this.scalePolicy) {
            stream.write(this._buildHealthReport());
          }
          // else: attached successfully, continue
        } else if (payload === 'execute') {
          this._handleExecute(msg.execute, stream);
        } else if (payload === 'cancel') {
          this._handleCancel(msg.cancel);
        } else if (payload === 'heartbeat') {
          // Server heartbeat — acknowledged silently
        } else if (payload === 'shutdown') {
          this.stop();
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
  // Attach message builder
  // -----------------------------------------------------------------------

  /**
   * Build the CasterAttach message object.
   * Extracted for testability — mirrors Rust SDK's build_attach_message().
   */
  private _buildAttachMessage(pilotId?: string): Record<string, unknown> {
    return {
      attach: {
        caster_id: this.casterId,
        runes: this._buildDeclarations(),
        labels: this._attachLabels(pilotId),
        max_concurrent: this.maxConcurrent,
        key: this.key || '',
        role: 'caster',
      },
    };
  }

  private _attachLabels(pilotId?: string): Record<string, string> {
    const labels: Record<string, string> = { ...this.labels };
    if (this.scalePolicy) {
      labels.group = this.scalePolicy.group;
      labels._scale_up = String(this.scalePolicy.scaleUpThreshold ?? 0.8);
      labels._scale_down = String(this.scalePolicy.scaleDownThreshold ?? 0.2);
      labels._sustained = String(this.scalePolicy.sustainedSecs ?? 30);
      labels._min = String(this.scalePolicy.minReplicas ?? 1);
      labels._max = String(this.scalePolicy.maxReplicas ?? 1);
      labels._spawn_command = this.scalePolicy.spawnCommand;
      labels._shutdown_signal = this.scalePolicy.shutdownSignal ?? 'SIGTERM';
      if (pilotId) {
        labels._pilot_id = pilotId;
      }
    }
    return labels;
  }

  private _buildHealthReport(): Record<string, unknown> {
    const metrics: Record<string, number> = {
      ...(this.loadReport?.metrics ?? {}),
      active_requests: this._activeRequests,
      max_concurrent: this.maxConcurrent,
      available_permits: Math.max(0, this.maxConcurrent - this._activeRequests),
    };
    const computedPressure =
      this.maxConcurrent === 0 ? 0 : this._activeRequests / this.maxConcurrent;
    const pressure = this.loadReport && this.loadReport.pressure > 0
      ? this.loadReport.pressure
      : computedPressure;
    return {
      health_report: {
        status: 'HEALTH_STATUS_HEALTHY',
        active_requests: this._activeRequests,
        error_rate: 0,
        custom_info: '',
        timestamp_ms: String(Date.now()),
        error_rate_window_secs: 0,
        pressure,
        metrics,
      },
    };
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
    this._activeRequests += 1;

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
        this._activeRequests = Math.max(0, this._activeRequests - 1);
      });
    } else {
      this._executeOnce(registered, ctx, req, stream, input).finally(() => {
        this._abortControllers.delete(req.request_id);
        this._activeRequests = Math.max(0, this._activeRequests - 1);
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
      let output: unknown;
      if (registered.acceptsFiles) {
        const handler = registered.handler as RuneHandlerWithFiles;
        output = await handler(ctx, input, ctx.attachments ?? []);
      } else {
        const handler = registered.handler as RuneHandler;
        output = await handler(ctx, input);
      }

      // If cancelled during execution, discard result
      if (ctx.signal.aborted) {
        return;
      }

      // Serialize output (handle null/undefined as empty JSON)
      const outputBuf =
        output == null
          ? Buffer.from('null')
          : output instanceof Buffer
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
      if (registered.acceptsFiles) {
        const handler = registered.handler as StreamRuneHandlerWithFiles;
        await handler(ctx, input, ctx.attachments ?? [], sender);
      } else {
        const handler = registered.handler as StreamRuneHandler;
        await handler(ctx, input, sender);
      }

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
