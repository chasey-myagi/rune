import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
export class PilotClient {
    pilotId;
    constructor(pilotId) {
        this.pilotId = pilotId;
    }
    static async ensure(runtime, key) {
        const normalized = normalizeRuntime(runtime);
        let response = null;
        try {
            response = await sendRequest({ command: 'status' });
        }
        catch {
            // No running pilot — will start one below.
        }
        if (response) {
            const status = classifyStatus(response, normalized);
            if (status.kind === 'ready')
                return new PilotClient(status.pilotId);
            if (status.kind === 'failed')
                throw new Error(status.error);
            if (status.kind === 'mismatch') {
                try {
                    await sendRequest({ command: 'stop' });
                }
                catch { /* ignore */ }
            }
            if (status.kind === 'retry') {
                return PilotClient.waitUntilReady(normalized);
            }
        }
        startPilot(runtime, key);
        return PilotClient.waitUntilReady(normalized, runtime, key);
    }
    /**
     * Poll until pilot reports ready. When `retryRuntime` is provided,
     * re-attempt start on connection failure so a slow predecessor release
     * doesn't doom the single initial spawn.
     */
    static async waitUntilReady(normalized, retryRuntime, retryKey) {
        const deadline = Date.now() + 5000;
        let lastStart = Date.now();
        while (Date.now() < deadline) {
            let response;
            try {
                response = await sendRequest({ command: 'status' });
            }
            catch {
                // Connection failed — re-attempt start if enough time has passed.
                if (retryRuntime && Date.now() - lastStart >= 1000) {
                    startPilot(retryRuntime, retryKey);
                    lastStart = Date.now();
                }
                await sleep(100);
                continue;
            }
            const status = classifyStatus(response, normalized);
            if (status.kind === 'ready')
                return new PilotClient(status.pilotId);
            if (status.kind === 'failed')
                throw new Error(status.error);
            await sleep(100);
        }
        throw new Error('pilot did not become ready');
    }
    async register(casterId, policy) {
        const response = await sendRequest({
            command: 'register',
            caster_id: casterId,
            pid: process.pid,
            group: policy.group,
            spawn_command: policy.spawnCommand,
            shutdown_signal: policy.shutdownSignal ?? 'SIGTERM',
        });
        ensureOk(response);
    }
    async deregister(casterId) {
        const response = await sendRequest({
            command: 'deregister',
            caster_id: casterId,
        });
        ensureOk(response);
    }
}
function ensureOk(response) {
    if (!response.ok) {
        throw new Error(response.error ?? 'pilot request failed');
    }
}
const PILOT_CONNECTING_ERROR = 'runtime session not attached';
function classifyStatus(response, normalized) {
    if (response.runtime !== normalized) {
        return { kind: 'mismatch' };
    }
    if (response.ok) {
        return { kind: 'ready', pilotId: response.pilot_id };
    }
    if (response.error === PILOT_CONNECTING_ERROR || !response.error) {
        return { kind: 'retry' };
    }
    return { kind: 'failed', error: response.error };
}
function startPilot(runtime, key) {
    const runeBin = findRuneBinary();
    const child = spawn(runeBin, ['pilot', 'daemon', '--runtime', normalizeRuntime(runtime)], {
        detached: true,
        stdio: 'ignore',
        env: key ? { ...process.env, RUNE_KEY: key } : process.env,
    });
    child.unref();
}
function sendRequest(request) {
    return new Promise((resolve, reject) => {
        const socket = net.createConnection(socketPath());
        const chunks = [];
        socket.once('connect', () => {
            socket.end(JSON.stringify(request));
        });
        socket.on('data', (chunk) => {
            chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
        });
        socket.once('error', reject);
        socket.once('end', () => {
            try {
                const response = JSON.parse(Buffer.concat(chunks).toString('utf8'));
                resolve(response);
            }
            catch (error) {
                reject(error);
            }
        });
    });
}
function socketPath() {
    return path.join(os.homedir(), '.rune', 'pilot.sock');
}
function findRuneBinary() {
    if (process.env.RUNE_BIN) {
        return process.env.RUNE_BIN;
    }
    const pathEntries = (process.env.PATH ?? '').split(path.delimiter);
    for (const entry of pathEntries) {
        const candidate = path.join(entry, 'rune');
        if (fs.existsSync(candidate)) {
            return candidate;
        }
    }
    throw new Error('failed to locate rune binary; set RUNE_BIN or add rune to PATH');
}
function normalizeRuntime(runtime) {
    return runtime.trim().replace(/\/+$/, '');
}
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}
//# sourceMappingURL=pilot-client.js.map