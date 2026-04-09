import type { ScalePolicy } from './types.js';
export declare class PilotClient {
    readonly pilotId: string;
    constructor(pilotId: string);
    static ensure(runtime: string, key?: string): Promise<PilotClient>;
    /**
     * Poll until pilot reports ready. When `retryRuntime` is provided,
     * re-attempt start on connection failure so a slow predecessor release
     * doesn't doom the single initial spawn.
     */
    private static waitUntilReady;
    register(casterId: string, policy: ScalePolicy): Promise<void>;
    deregister(casterId: string): Promise<void>;
}
