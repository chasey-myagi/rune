import * as fs from 'fs';
import * as net from 'net';
import * as os from 'os';
import * as path from 'path';
import { spawn } from 'child_process';
import type { ScalePolicy } from './types.js';

type PilotRequest =
  | {
      command: 'register';
      caster_id: string;
      pid: number;
      group: string;
      spawn_command: string;
      shutdown_signal: string;
    }
  | {
      command: 'deregister';
      caster_id: string;
    }
  | {
      command: 'status';
    }
  | {
      command: 'stop';
    };

interface PilotResponse {
  ok: boolean;
  pilot_id: string;
  runtime: string;
  error?: string | null;
}

export class PilotClient {
  constructor(readonly pilotId: string) {}

  static async ensure(runtime: string, key?: string): Promise<PilotClient> {
    const normalized = normalizeRuntime(runtime);
    let response: PilotResponse | null = null;
    try {
      response = await sendRequest({ command: 'status' });
    } catch {
      // No running pilot — will start one below.
    }

    if (response) {
      const status = classifyStatus(response, normalized);
      if (status.kind === 'ready') return new PilotClient(status.pilotId);
      if (status.kind === 'failed') throw new Error(status.error);
      if (status.kind === 'mismatch') {
        try { await sendRequest({ command: 'stop' }); } catch { /* ignore */ }
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
  private static async waitUntilReady(
    normalized: string,
    retryRuntime?: string,
    retryKey?: string,
  ): Promise<PilotClient> {
    const deadline = Date.now() + pilotEnsureTimeoutMs();
    let lastStart = Date.now();
    while (Date.now() < deadline) {
      let response: PilotResponse;
      try {
        response = await sendRequest({ command: 'status' });
      } catch {
        // Connection failed — re-attempt start if enough time has passed.
        if (retryRuntime && Date.now() - lastStart >= 1000) {
          startPilot(retryRuntime, retryKey);
          lastStart = Date.now();
        }
        await sleep(100);
        continue;
      }
      const status = classifyStatus(response, normalized);
      if (status.kind === 'ready') return new PilotClient(status.pilotId);
      if (status.kind === 'failed') throw new Error(status.error);
      await sleep(100);
    }
    throw new Error('pilot did not become ready');
  }

  async register(casterId: string, policy: ScalePolicy): Promise<void> {
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

  async deregister(casterId: string): Promise<void> {
    const response = await sendRequest({
      command: 'deregister',
      caster_id: casterId,
    });
    ensureOk(response);
  }

}

function ensureOk(response: PilotResponse): void {
  if (!response.ok) {
    throw new Error(response.error ?? 'pilot request failed');
  }
}

type EnsureStatus =
  | { kind: 'ready'; pilotId: string }
  | { kind: 'retry' }
  | { kind: 'mismatch' }
  | { kind: 'failed'; error: string };

const PILOT_CONNECTING_ERROR = 'runtime session not attached';

function classifyStatus(response: PilotResponse, normalized: string): EnsureStatus {
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

function startPilot(runtime: string, key?: string): void {
  const runeBin = findRuneBinary();
  const child = spawn(
    runeBin,
    ['pilot', 'daemon', '--runtime', normalizeRuntime(runtime)],
    {
      detached: true,
      stdio: 'ignore',
      env: key ? { ...process.env, RUNE_KEY: key } : process.env,
    },
  );
  child.unref();
}

const DEFAULT_PILOT_ENSURE_TIMEOUT_MS = 5000;
const DEFAULT_PILOT_REQUEST_TIMEOUT_MS = 5000;

function pilotEnsureTimeoutMs(): number {
  const env = process.env.RUNE_PILOT_ENSURE_TIMEOUT_SECS;
  if (!env) return DEFAULT_PILOT_ENSURE_TIMEOUT_MS;
  const parsed = Number(env);
  return isNaN(parsed) ? DEFAULT_PILOT_ENSURE_TIMEOUT_MS : parsed * 1000;
}

function pilotRequestTimeoutMs(): number {
  const env = process.env.RUNE_PILOT_REQUEST_TIMEOUT_SECS;
  if (!env) return DEFAULT_PILOT_REQUEST_TIMEOUT_MS;
  const parsed = Number(env);
  return isNaN(parsed) ? DEFAULT_PILOT_REQUEST_TIMEOUT_MS : parsed * 1000;
}

function sendRequest(request: PilotRequest): Promise<PilotResponse> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection(socketPath());
    const chunks: Buffer[] = [];

    socket.setTimeout(pilotRequestTimeoutMs());
    socket.on('timeout', () => {
      socket.destroy(new Error('pilot socket timeout'));
    });
    socket.once('connect', () => {
      socket.end(JSON.stringify(request));
    });
    socket.on('data', (chunk) => {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    });
    socket.once('error', reject);
    socket.once('end', () => {
      try {
        const response = JSON.parse(Buffer.concat(chunks).toString('utf8')) as PilotResponse;
        resolve(response);
      } catch (error) {
        reject(error);
      }
    });
  });
}

function socketPath(): string {
  return path.join(os.homedir(), '.rune', 'pilot.sock');
}

function findRuneBinary(): string {
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

function normalizeRuntime(runtime: string): string {
  return runtime.trim().replace(/\/+$/, '');
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
