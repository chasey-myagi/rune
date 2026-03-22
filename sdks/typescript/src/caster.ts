import type { CasterOptions, RuneConfig, ReconnectOptions } from './types.js';
import type { RuneHandler, StreamRuneHandler } from './handler.js';

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
  handler: RuneHandler | StreamRuneHandler;
  isStream: boolean;
}

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
  readonly heartbeatIntervalMs: number;
  readonly maxConcurrent: number;
  readonly labels: Record<string, string>;
  readonly reconnect: Required<ReconnectOptions>;

  private _runes: Map<string, RegisteredRune> = new Map();

  constructor(options: CasterOptions) {
    this.runtime = options.runtime ?? DEFAULT_RUNTIME;
    this.key = options.key;
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
  rune(config: RuneConfig, handler: RuneHandler): void {
    if (this._runes.has(config.name)) {
      throw new Error(`Rune "${config.name}" is already registered`);
    }
    this._runes.set(config.name, { config, handler, isStream: false });
  }

  /**
   * Register a streaming Rune handler.
   * @throws Error if a Rune with the same name is already registered
   */
  streamRune(config: RuneConfig, handler: StreamRuneHandler): void {
    if (this._runes.has(config.name)) {
      throw new Error(`Rune "${config.name}" is already registered`);
    }
    this._runes.set(config.name, {
      config: { ...config, supportsStream: true },
      handler,
      isStream: true,
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
   * Start the Caster: connect to Runtime, send Attach, and begin handling requests.
   * Returns a Promise that resolves when the Caster is shut down.
   */
  async run(): Promise<void> {
    // TODO: establish gRPC bidirectional stream
    // TODO: send CasterAttach with registered runes
    // TODO: handle incoming ExecuteRequest messages
    // TODO: heartbeat loop
    // TODO: reconnection with exponential backoff
    throw new Error('not implemented');
  }
}
