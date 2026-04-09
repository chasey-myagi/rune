import type { ScalePolicy } from './types.js';
export declare class PilotClient {
    readonly pilotId: string;
    constructor(pilotId: string);
    static ensure(runtime: string, key?: string): Promise<PilotClient>;
    register(casterId: string, policy: ScalePolicy): Promise<void>;
    deregister(casterId: string): Promise<void>;
    private static fromResponse;
}
