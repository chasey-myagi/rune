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
        try {
            const response = await sendRequest({ command: 'status' });
            return PilotClient.fromResponse(response);
        }
        catch {
            startPilot(runtime, key);
        }
        const deadline = Date.now() + 5000;
        while (Date.now() < deadline) {
            try {
                const response = await sendRequest({ command: 'status' });
                return PilotClient.fromResponse(response);
            }
            catch {
                await sleep(100);
            }
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
    static fromResponse(response) {
        ensureOk(response);
        return new PilotClient(response.pilot_id);
    }
}
function ensureOk(response) {
    if (!response.ok) {
        throw new Error(response.error ?? 'pilot request failed');
    }
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