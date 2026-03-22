import type { CasterOptions, RuneConfig, ReconnectOptions } from './types.js';
import type { RuneHandler, StreamRuneHandler } from './handler.js';
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
export declare class Caster {
    readonly runtime: string;
    readonly key: string;
    readonly heartbeatIntervalMs: number;
    readonly maxConcurrent: number;
    readonly labels: Record<string, string>;
    readonly reconnect: Required<ReconnectOptions>;
    private _runes;
    private _stopped;
    private _abortControllers;
    constructor(options: CasterOptions);
    /**
     * Register a unary Rune handler.
     * @throws Error if a Rune with the same name is already registered
     */
    rune(config: RuneConfig, handler: RuneHandler): void;
    /**
     * Register a streaming Rune handler.
     * @throws Error if a Rune with the same name is already registered
     */
    streamRune(config: RuneConfig, handler: StreamRuneHandler): void;
    /**
     * Returns the number of registered Runes.
     */
    get runeCount(): number;
    /**
     * Returns the config of a registered Rune by name, or undefined.
     */
    getRuneConfig(name: string): RuneConfig | undefined;
    /**
     * Check if a rune is registered as a stream handler.
     */
    isStreamRune(name: string): boolean;
    /**
     * Stop the Caster. Current session will end and no reconnection will happen.
     */
    stop(): void;
    /**
     * Start the Caster: connect to Runtime, send Attach, and begin handling requests.
     * Returns a Promise that resolves when the Caster is shut down.
     *
     * Auto-reconnects with exponential backoff on errors (unless reconnect.enabled is false).
     */
    run(): Promise<void>;
    private _connectAndRun;
    private _buildDeclarations;
    private _handleExecute;
    private _executeOnce;
    private _executeStream;
    private _handleCancel;
    private _sleep;
}
