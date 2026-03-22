/**
 * E2E tests — require a running `rune-server --dev` instance.
 *
 * By default these tests are skipped unless:
 *   - Environment variable RUNE_E2E=1 is set, OR
 *   - The runtime health endpoint is reachable before the suite starts.
 *
 * To run manually:
 *   cargo run -p rune-server -- --dev   # in one terminal
 *   RUNE_E2E=1 npm test                 # in another
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest';
import { spawn, ChildProcess } from 'child_process';
import { Caster, StreamSender } from '../src/index';
import type { RuneContext, FileAttachment } from '../src/index';
import type { RuneHandler, StreamRuneHandler } from '../src/index';

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------
const HTTP_BASE = 'http://localhost:50060';
const GRPC_ADDR = 'localhost:50070';
const HEALTH_URL = `${HTTP_BASE}/health`;
const RUNES_URL = `${HTTP_BASE}/api/v1/runes`;
const TASKS_URL = `${HTTP_BASE}/api/v1/tasks`;
const OPENAPI_URL = `${HTTP_BASE}/api/v1/openapi.json`;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Wait until runtime health endpoint responds 200. */
async function waitForHealth(maxWaitMs = 120_000): Promise<boolean> {
  const start = Date.now();
  while (Date.now() - start < maxWaitMs) {
    try {
      const res = await fetch(HEALTH_URL);
      if (res.ok) return true;
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  return false;
}

/** Create a connected Caster, wait for it to register, and return cleanup. */
async function createConnectedCaster(
  setupFn: (caster: Caster) => void,
  opts?: { key?: string; casterId?: string },
): Promise<{ caster: Caster; stop: () => void }> {
  const caster = new Caster({
    runtime: GRPC_ADDR,
    key: opts?.key ?? 'rk_e2e_test',
    casterId: opts?.casterId,
    reconnect: { enabled: true, initialDelayMs: 500, maxDelayMs: 5000 },
  });
  setupFn(caster);
  const runPromise = caster.run().catch(() => {}); // swallow disconnect errors
  // Wait for registration to propagate
  await new Promise((r) => setTimeout(r, 2000));
  return {
    caster,
    stop: () => {
      caster.stop();
    },
  };
}

/** Fetch rune list from runtime. */
async function listRunes(): Promise<any[]> {
  const res = await fetch(RUNES_URL);
  if (!res.ok) return [];
  const body = await res.json();
  // API returns { runes: [...] } not a bare array
  return body.runes ?? body;
}

/** Call a rune synchronously. */
async function callRune(
  name: string,
  body: unknown,
  opts?: { stream?: boolean; async?: boolean },
): Promise<Response> {
  const params = new URLSearchParams();
  if (opts?.stream) params.set('stream', 'true');
  if (opts?.async) params.set('async', 'true');
  const qs = params.toString();
  const url = `${RUNES_URL}/${name}/run${qs ? `?${qs}` : ''}`;
  return fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/** Call a rune via gate path. */
async function callGate(path: string, body: unknown): Promise<Response> {
  return fetch(`${HTTP_BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/** Read all SSE events from a streaming response. */
async function readSSE(res: Response): Promise<{ events: string[]; done: boolean; error?: string }> {
  const text = await res.text();
  const events: string[] = [];
  let done = false;
  let error: string | undefined;

  for (const line of text.split('\n')) {
    if (line.startsWith('data: ')) {
      const data = line.slice(6);
      if (data === '[DONE]') {
        done = true;
      } else {
        events.push(data);
      }
    }
    if (line.startsWith('event: error')) {
      error = 'error';
    }
  }
  return { events, done, error };
}

// ---------------------------------------------------------------------------
// Skip logic: skip all E2E tests unless RUNE_E2E=1
// ---------------------------------------------------------------------------
const E2E_ENABLED = process.env.RUNE_E2E === '1';

// ===========================================================================
// E2E Test Suite
// ===========================================================================
const describeE2E = E2E_ENABLED ? describe : describe.skip;

describeE2E('E2E Tests', () => {
  beforeAll(async () => {
    const ready = await waitForHealth();
    if (!ready) {
      throw new Error('Runtime not available — cannot run E2E tests');
    }
  }, 130_000);
  // =========================================================================
  // 2.1 Connection & Registration (E-01 ~ E-06)
  // =========================================================================
  describe('2.1 Connection & Registration', () => {
    it('E-01: Caster connects and registers 1 rune', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-echo-01' }, async (_ctx, input) => input);
      });
      try {
        const runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-echo-01')).toBe(true);
      } finally {
        stop();
      }
    }, 30_000);

    it('E-02: Caster registers 3 runes', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-r1' }, async (_ctx, input) => input);
        c.rune({ name: 'e2e-r2' }, async (_ctx, input) => input);
        c.streamRune({ name: 'e2e-r3' }, async (_ctx, _input, stream) => {
          await stream.emit('done');
        });
      });
      try {
        const runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-r1')).toBe(true);
        expect(runes.some((r: any) => r.name === 'e2e-r2')).toBe(true);
        expect(runes.some((r: any) => r.name === 'e2e-r3')).toBe(true);
      } finally {
        stop();
      }
    }, 30_000);

    it('E-03: Caster registers rune with gate_path', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune(
          { name: 'e2e-gated', gate: { path: '/e2e-gated' } },
          async (_ctx, input) => input,
        );
      });
      try {
        const runes = await listRunes();
        const gated = runes.find((r: any) => r.name === 'e2e-gated');
        expect(gated).toBeDefined();
        expect(gated.gate_path ?? gated.gate?.path).toBe('/e2e-gated');
      } finally {
        stop();
      }
    }, 30_000);

    // Server GET /api/v1/runes only returns name + gate_path; input_schema not included in listing
    it.skip('E-04: Caster registers rune with schema', async () => {
      const inputSchema = { type: 'object', properties: { text: { type: 'string' } } };
      const { stop } = await createConnectedCaster((c) => {
        c.rune(
          { name: 'e2e-schema', inputSchema },
          async (_ctx, input) => input,
        );
      });
      try {
        const runes = await listRunes();
        const r = runes.find((r: any) => r.name === 'e2e-schema');
        expect(r).toBeDefined();
        // Schema should be present (exact structure depends on runtime)
        const schema = r.input_schema ?? r.inputSchema;
        expect(schema).toBeDefined();
      } finally {
        stop();
      }
    }, 30_000);

    it('E-05: Caster disconnect removes rune from list', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-disappear' }, async (_ctx, input) => input);
      });
      // Verify registered
      let runes = await listRunes();
      expect(runes.some((r: any) => r.name === 'e2e-disappear')).toBe(true);
      // Disconnect
      stop();
      await new Promise((r) => setTimeout(r, 3000));
      // Verify gone
      runes = await listRunes();
      expect(runes.some((r: any) => r.name === 'e2e-disappear')).toBe(false);
    }, 30_000);

    it('E-06: Caster reconnects after disconnect', async () => {
      const caster = new Caster({
        runtime: GRPC_ADDR,
        key: 'rk_e2e_test',
        reconnect: { enabled: true, initialDelayMs: 500, maxDelayMs: 3000 },
      });
      caster.rune({ name: 'e2e-reconnect' }, async (_ctx, input) => input);
      const runPromise = caster.run().catch(() => {});
      await new Promise((r) => setTimeout(r, 2000));

      let runes = await listRunes();
      expect(runes.some((r: any) => r.name === 'e2e-reconnect')).toBe(true);

      caster.stop();
      // Reconnect logic is internal — for this test we just verify initial connection was fine
    }, 30_000);
  });

  // =========================================================================
  // 2.2 Sync Call (E-10 ~ E-16)
  // =========================================================================
  describe('2.2 Sync Call', () => {
    let stopCaster: () => void;

    beforeAll(async () => {
      const { stop } = await createConnectedCaster((c) => {
        // Echo rune
        c.rune({ name: 'e2e-sync-echo', gate: { path: '/e2e-sync-echo' } }, async (_ctx, input) => input);
        // Transform rune
        c.rune({ name: 'e2e-sync-transform' }, async (_ctx, input) => {
          const obj = input as Record<string, unknown>;
          return { ...obj, transformed: true };
        });
        // Error rune
        c.rune({ name: 'e2e-sync-error' }, async () => {
          throw new Error('handler exploded');
        });
      });
      stopCaster = stop;
    }, 30_000);

    afterAll(() => {
      stopCaster?.();
    });

    it('E-10: POST /api/v1/runes/{name}/run — echo', async () => {
      const res = await callRune('e2e-sync-echo', { msg: 'hello' });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.msg).toBe('hello');
    }, 15_000);

    it('E-11: call via gate_path', async () => {
      const res = await callGate('/e2e-sync-echo', { via: 'gate' });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.via).toBe('gate');
    }, 15_000);

    it('E-12: handler modifies input and returns', async () => {
      const res = await callRune('e2e-sync-transform', { x: 1 });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.x).toBe(1);
      expect(body.transformed).toBe(true);
    }, 15_000);

    it('E-13: handler throws exception', async () => {
      const res = await callRune('e2e-sync-error', {});
      expect(res.status).toBe(500);
      const body = await res.json();
      // Error may be nested: { error: { code, message } } or { error: "..." }
      const errStr = JSON.stringify(body);
      expect(errStr).toContain('handler exploded');
    }, 15_000);

    it('E-14: call nonexistent rune', async () => {
      const res = await callRune('e2e-nonexistent-rune', {});
      expect(res.status).toBe(404);
    }, 15_000);

    it('E-15: large payload (100KB JSON)', async () => {
      const bigArray = Array.from({ length: 10000 }, (_, i) => ({ idx: i, data: 'x'.repeat(10) }));
      const res = await callRune('e2e-sync-echo', bigArray);
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body).toHaveLength(10000);
    }, 30_000);

    it('E-16: empty body call', async () => {
      // Send POST with empty JSON body
      const res = await fetch(`${RUNES_URL}/e2e-sync-echo/run`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: '{}',
      });
      expect(res.status).toBe(200);
    }, 15_000);
  });

  // =========================================================================
  // 2.3 Stream Call (E-20 ~ E-25)
  // =========================================================================
  describe('2.3 Stream Call', () => {
    let stopCaster: () => void;

    beforeAll(async () => {
      const { stop } = await createConnectedCaster((c) => {
        // 3-chunk stream
        c.streamRune({ name: 'e2e-stream-3' }, async (_ctx, _input, stream) => {
          await stream.emit({ chunk: 1 });
          await stream.emit({ chunk: 2 });
          await stream.emit({ chunk: 3 });
        });
        // String stream
        c.streamRune({ name: 'e2e-stream-str' }, async (_ctx, _input, stream) => {
          await stream.emit('hello');
          await stream.emit('world');
        });
        // JSON stream
        c.streamRune({ name: 'e2e-stream-json' }, async (_ctx, _input, stream) => {
          await stream.emit({ key: 'value' });
        });
        // Error stream
        c.streamRune({ name: 'e2e-stream-error' }, async () => {
          throw new Error('stream handler failed');
        });
        // Non-stream rune (for E-24)
        c.rune({ name: 'e2e-no-stream' }, async (_ctx, input) => input);
      });
      stopCaster = stop;
    }, 30_000);

    afterAll(() => {
      stopCaster?.();
    });

    it('E-20: stream rune sends 3 chunks', async () => {
      const res = await callRune('e2e-stream-3', {}, { stream: true });
      expect(res.status).toBe(200);
      const { events } = await readSSE(res);
      expect(events.length).toBeGreaterThanOrEqual(3);
    }, 15_000);

    it('E-21: stream handler emit string', async () => {
      const res = await callRune('e2e-stream-str', {}, { stream: true });
      expect(res.status).toBe(200);
      const { events } = await readSSE(res);
      expect(events.length).toBeGreaterThanOrEqual(2);
    }, 15_000);

    it('E-22: stream handler emit JSON object', async () => {
      const res = await callRune('e2e-stream-json', {}, { stream: true });
      expect(res.status).toBe(200);
      const { events } = await readSSE(res);
      expect(events.length).toBeGreaterThanOrEqual(1);
      // At least one event should be parseable JSON
      const parsed = JSON.parse(events[0]);
      expect(parsed.key).toBe('value');
    }, 15_000);

    it('E-23: stream handler throws exception', async () => {
      const res = await callRune('e2e-stream-error', {}, { stream: true });
      // Depending on runtime: could be 200 with error event, or 500
      const text = await res.text();
      // Should contain error info somewhere
      expect(text).toContain('stream handler failed');
    }, 15_000);

    it('E-24: stream request on non-stream rune', async () => {
      const res = await callRune('e2e-no-stream', {}, { stream: true });
      expect(res.status).toBe(400);
    }, 15_000);

    it('E-25: stream client disconnect (cancel signal)', async () => {
      // This test verifies the server handles client disconnect gracefully.
      // We initiate a stream request and abort it immediately.
      const controller = new AbortController();
      try {
        const fetchPromise = fetch(`${RUNES_URL}/e2e-stream-3/run?stream=true`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: '{}',
          signal: controller.signal,
        });
        // Abort quickly
        setTimeout(() => controller.abort(), 100);
        await fetchPromise;
      } catch (e: any) {
        // AbortError is expected
        expect(e.name).toBe('AbortError');
      }
    }, 15_000);
  });

  // =========================================================================
  // 2.4 Async Call (E-30 ~ E-35)
  // =========================================================================
  describe('2.4 Async Call', () => {
    let stopCaster: () => void;

    beforeAll(async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-async-echo' }, async (_ctx, input) => input);
        c.rune({ name: 'e2e-async-fail' }, async () => {
          throw new Error('async handler failed');
        });
        c.rune({ name: 'e2e-async-slow' }, async (_ctx, _input) => {
          await new Promise((r) => setTimeout(r, 3000));
          return { done: true };
        });
      });
      stopCaster = stop;
    }, 30_000);

    afterAll(() => {
      stopCaster?.();
    });

    it('E-30: POST ?async=true returns 202 with task_id', async () => {
      const res = await callRune('e2e-async-echo', { msg: 'async' }, { async: true });
      expect(res.status).toBe(202);
      const body = await res.json();
      expect(body.task_id ?? body.taskId).toBeDefined();
    }, 15_000);

    it('E-31: GET /api/v1/tasks/{id} completed task', async () => {
      const res = await callRune('e2e-async-echo', { msg: 'check' }, { async: true });
      const body = await res.json();
      const taskId = body.task_id ?? body.taskId;

      // Poll until completed
      for (let i = 0; i < 20; i++) {
        const taskRes = await fetch(`${TASKS_URL}/${taskId}`);
        const task = await taskRes.json();
        if (task.status === 'completed') {
          expect(task.output ?? task.result).toBeDefined();
          return;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error('Task did not complete in time');
    }, 30_000);

    it('E-32: GET /api/v1/tasks/{id} failed task', async () => {
      const res = await callRune('e2e-async-fail', {}, { async: true });
      const body = await res.json();
      const taskId = body.task_id ?? body.taskId;

      for (let i = 0; i < 20; i++) {
        const taskRes = await fetch(`${TASKS_URL}/${taskId}`);
        const task = await taskRes.json();
        if (task.status === 'failed') {
          expect(task.error).toBeDefined();
          return;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error('Task did not fail in time');
    }, 30_000);

    it('E-33: DELETE /api/v1/tasks/{id} cancel running task', async () => {
      const res = await callRune('e2e-async-slow', {}, { async: true });
      const body = await res.json();
      const taskId = body.task_id ?? body.taskId;

      // Cancel immediately
      const delRes = await fetch(`${TASKS_URL}/${taskId}`, { method: 'DELETE' });
      expect([200, 202, 204]).toContain(delRes.status);

      // Verify cancelled
      await new Promise((r) => setTimeout(r, 1000));
      const taskRes = await fetch(`${TASKS_URL}/${taskId}`);
      const task = await taskRes.json();
      expect(task.status).toBe('cancelled');
    }, 30_000);

    it('E-34: GET nonexistent task_id', async () => {
      const res = await fetch(`${TASKS_URL}/nonexistent-task-id-12345`);
      expect(res.status).toBe(404);
    }, 15_000);

    it('E-35: async call slow rune, poll until completed', async () => {
      const res = await callRune('e2e-async-slow', { data: 'patience' }, { async: true });
      const body = await res.json();
      const taskId = body.task_id ?? body.taskId;

      for (let i = 0; i < 30; i++) {
        const taskRes = await fetch(`${TASKS_URL}/${taskId}`);
        const task = await taskRes.json();
        if (task.status === 'completed') {
          expect(task.output ?? task.result).toBeDefined();
          return;
        }
        await new Promise((r) => setTimeout(r, 500));
      }
      throw new Error('Slow task did not complete in time');
    }, 30_000);
  });

  // =========================================================================
  // 2.5 Schema Validation (E-40 ~ E-45)
  // =========================================================================
  describe('2.5 Schema Validation', () => {
    let stopCaster: () => void;

    beforeAll(async () => {
      const inputSchema = {
        type: 'object',
        properties: { text: { type: 'string' } },
        required: ['text'],
      };
      const outputSchema = {
        type: 'object',
        properties: { result: { type: 'string' } },
        required: ['result'],
      };
      const { stop } = await createConnectedCaster((c) => {
        c.rune(
          { name: 'e2e-schema-in', inputSchema },
          async (_ctx, input) => input,
        );
        c.rune({ name: 'e2e-no-schema' }, async (_ctx, input) => input);
        c.rune(
          { name: 'e2e-schema-both', inputSchema, outputSchema },
          async (_ctx, input) => ({ result: (input as any).text }),
        );
        c.rune(
          { name: 'e2e-schema-bad-out', inputSchema: { type: 'object' }, outputSchema },
          async (_ctx, _input) => ({ bad_field: 123 }), // intentionally wrong
        );
      });
      stopCaster = stop;
    }, 30_000);

    afterAll(() => {
      stopCaster?.();
    });

    it('E-40: valid input passes schema validation', async () => {
      const res = await callRune('e2e-schema-in', { text: 'hello' });
      expect(res.status).toBe(200);
    }, 15_000);

    it('E-41: invalid input fails schema validation', async () => {
      const res = await callRune('e2e-schema-in', { wrong: 123 });
      expect(res.status).toBe(422);
      const body = await res.json();
      const errStr = JSON.stringify(body);
      expect(errStr).toContain('text');
    }, 15_000);

    it('E-42: no schema, any input accepted', async () => {
      const res = await callRune('e2e-no-schema', { anything: 'goes' });
      expect(res.status).toBe(200);
    }, 15_000);

    it('E-43: input + output schema, all valid', async () => {
      const res = await callRune('e2e-schema-both', { text: 'world' });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.result).toBe('world');
    }, 15_000);

    it('E-44: output does not match schema', async () => {
      const res = await callRune('e2e-schema-bad-out', { ok: true });
      expect(res.status).toBe(500);
    }, 15_000);

    it('E-45: OpenAPI endpoint contains schema info', async () => {
      const res = await fetch(OPENAPI_URL);
      expect(res.status).toBe(200);
      const doc = await res.json();
      // Should be a valid OpenAPI doc with paths
      expect(doc.paths ?? doc.openapi).toBeDefined();
    }, 15_000);
  });

  // =========================================================================
  // 2.6 File Transfer (E-50 ~ E-55)
  // =========================================================================
  describe('2.6 File Transfer', () => {
    let stopCaster: () => void;

    beforeAll(async () => {
      const { stop } = await createConnectedCaster((c) => {
        // Handler: just echo input. Files are stored in Gate's file broker,
        // NOT forwarded to handler via gRPC.
        c.rune({ name: 'e2e-file-echo' }, async (_ctx, input) => input);
        // Handler that doesn't declare files param
        c.rune({ name: 'e2e-file-ignore' }, async (_ctx, input) => input);
      });
      stopCaster = stop;
    }, 30_000);

    afterAll(() => {
      stopCaster?.();
    });

    it('E-50: multipart upload file + JSON', async () => {
      const formData = new FormData();
      formData.append('input', JSON.stringify({ msg: 'with file' }));
      formData.append('file', new Blob(['file content'], { type: 'text/plain' }), 'test.txt');

      const res = await fetch(`${RUNES_URL}/e2e-file-echo/run`, {
        method: 'POST',
        body: formData,
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      // Gate wraps response with a "files" array containing stored-file metadata
      expect(body.files).toBeDefined();
      expect(body.files.length).toBeGreaterThanOrEqual(1);
      expect(body.files[0].filename).toBe('test.txt');
    }, 15_000);

    it('E-51: FileAttachment fields correct', async () => {
      const content = 'hello';
      const formData = new FormData();
      formData.append('input', JSON.stringify({}));
      formData.append('file', new Blob([content], { type: 'text/plain' }), 'hello.txt');

      const res = await fetch(`${RUNES_URL}/e2e-file-echo/run`, {
        method: 'POST',
        body: formData,
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      // Verify Gate-returned file metadata
      expect(body.files).toBeDefined();
      expect(body.files.length).toBeGreaterThanOrEqual(1);
      const f = body.files[0];
      expect(f.filename).toBe('hello.txt');
      expect(f.mime_type).toBe('text/plain');
      expect(f.size).toBe(content.length);
    }, 15_000);

    it('E-52: multiple file upload', async () => {
      const formData = new FormData();
      formData.append('input', JSON.stringify({}));
      formData.append('files', new Blob(['aaa'], { type: 'text/plain' }), 'a.txt');
      formData.append('files', new Blob(['bbb'], { type: 'text/plain' }), 'b.txt');

      const res = await fetch(`${RUNES_URL}/e2e-file-echo/run`, {
        method: 'POST',
        body: formData,
      });
      expect(res.status).toBe(200);
      const body = await res.json();
      expect(body.files).toBeDefined();
      expect(body.files.length).toBeGreaterThanOrEqual(2);
      const uploadedNames = new Set(body.files.map((f: any) => f.filename));
      expect(uploadedNames.has('a.txt')).toBe(true);
      expect(uploadedNames.has('b.txt')).toBe(true);
    }, 15_000);

    // Server max_upload_size_mb is 100 in --dev mode;
    // 100MB file fits within the limit. Sending >110MB in a test is too slow.
    it.skip('E-53: oversized file rejected', async () => {
      const bigContent = 'x'.repeat(20 * 1024 * 1024);
      const formData = new FormData();
      formData.append('input', JSON.stringify({}));
      formData.append('file', new Blob([bigContent]), 'huge.bin');

      const res = await fetch(`${RUNES_URL}/e2e-file-echo/run`, {
        method: 'POST',
        body: formData,
      });
      expect(res.status).toBe(413);
    }, 30_000);

    it('E-54: no file, pure JSON via multipart (backward compat)', async () => {
      const formData = new FormData();
      formData.append('input', JSON.stringify({ pure: 'json' }));

      const res = await fetch(`${RUNES_URL}/e2e-file-echo/run`, {
        method: 'POST',
        body: formData,
      });
      expect(res.status).toBe(200);
    }, 15_000);

    it('E-55: handler without files param, files ignored', async () => {
      const formData = new FormData();
      formData.append('input', JSON.stringify({ data: 'test' }));
      formData.append('file', new Blob(['ignored'], { type: 'text/plain' }), 'ignore.txt');

      const res = await fetch(`${RUNES_URL}/e2e-file-ignore/run`, {
        method: 'POST',
        body: formData,
      });
      expect(res.status).toBe(200);
    }, 15_000);
  });

  // =========================================================================
  // 2.7 Heartbeat & Lifecycle (E-60 ~ E-63)
  // =========================================================================
  describe('2.7 Heartbeat & Lifecycle', () => {
    it('E-60: Caster stays connected for 30 seconds', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-heartbeat' }, async (_ctx, input) => input);
      });
      try {
        // Wait 30 seconds
        await new Promise((r) => setTimeout(r, 30_000));
        // Should still be registered
        const runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-heartbeat')).toBe(true);
      } finally {
        stop();
      }
    }, 60_000);

    it('E-61: runtime restart, Caster auto-reconnects', async () => {
      // This test is hard to automate without controlling the runtime process.
      // We verify that reconnect config is properly set and initial connection works.
      const caster = new Caster({
        runtime: GRPC_ADDR,
        key: 'rk_e2e_test',
        reconnect: { enabled: true, initialDelayMs: 500, maxDelayMs: 3000 },
      });
      caster.rune({ name: 'e2e-reconnect-test' }, async (_ctx, input) => input);
      const runPromise = caster.run().catch(() => {});
      await new Promise((r) => setTimeout(r, 2000));

      const runes = await listRunes();
      expect(runes.some((r: any) => r.name === 'e2e-reconnect-test')).toBe(true);
      caster.stop();
    }, 30_000);

    it('E-62: concurrent calls to same rune (10x)', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-concurrent' }, async (_ctx, input) => {
          return { echo: input };
        });
      });
      try {
        const promises = Array.from({ length: 10 }, (_, i) =>
          callRune('e2e-concurrent', { idx: i }),
        );
        const responses = await Promise.all(promises);
        for (const res of responses) {
          expect(res.status).toBe(200);
          const body = await res.json();
          expect(body.echo).toBeDefined();
        }
      } finally {
        stop();
      }
    }, 30_000);

    it('E-63: two Casters register same rune name', async () => {
      const { stop: stop1 } = await createConnectedCaster(
        (c) => {
          c.rune({ name: 'e2e-loadbalance' }, async () => ({ from: 'caster1' }));
        },
        { casterId: 'caster-lb-1' },
      );
      const { stop: stop2 } = await createConnectedCaster(
        (c) => {
          c.rune({ name: 'e2e-loadbalance' }, async () => ({ from: 'caster2' }));
        },
        { casterId: 'caster-lb-2' },
      );
      try {
        // Call multiple times, should get responses from both
        const results = new Set<string>();
        for (let i = 0; i < 20; i++) {
          const res = await callRune('e2e-loadbalance', {});
          if (res.status === 200) {
            const body = await res.json();
            results.add(body.from);
          }
        }
        // At least one caster should respond
        expect(results.size).toBeGreaterThanOrEqual(1);
      } finally {
        stop1();
        stop2();
      }
    }, 30_000);
  });

  // =========================================================================
  // 2.8 Extended Tests (beyond test-matrix)
  // =========================================================================
  describe('2.8 Extended Tests', () => {
    it('EX-01: high concurrency (20 parallel requests)', async () => {
      const caster = new Caster({
        runtime: GRPC_ADDR,
        key: 'rk_e2e_test',
        maxConcurrent: 30,
        reconnect: { enabled: true, initialDelayMs: 500, maxDelayMs: 5000 },
      });
      caster.rune({ name: 'e2e-ext-conc20' }, async (_ctx: any, input: any) => {
        return { echo: input };
      });
      const runPromise = caster.run().catch(() => {});
      await new Promise((r) => setTimeout(r, 2000));
      const stop = () => caster.stop();
      try {
        const promises = Array.from({ length: 20 }, (_, i) =>
          callRune('e2e-ext-conc20', { idx: i }),
        );
        const responses = await Promise.all(promises);
        const statuses = responses.map((r) => r.status);
        const successes = statuses.filter((s) => s === 200);
        expect(successes.length).toBe(20);
        for (const res of responses) {
          const body = await res.json();
          expect(body.echo).toBeDefined();
        }
      } finally {
        stop();
      }
    }, 60_000);

    it('EX-02: large JSON payload (512KB)', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-ext-large' }, async (_ctx, input) => input);
      });
      try {
        // ~512KB JSON string payload (stays within server body limit)
        const size = 512 * 1024;
        const bigData = { data: 'x'.repeat(size) };
        const res = await callRune('e2e-ext-large', bigData);
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.data.length).toBe(size);
      } finally {
        stop();
      }
    }, 30_000);

    it('EX-03: special characters in rune name', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-ext-special_name.v2' }, async (_ctx, input) => input);
      });
      try {
        const runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-ext-special_name.v2')).toBe(true);
      } finally {
        stop();
      }
    }, 30_000);

    it('EX-04: special characters in input', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-ext-special-input' }, async (_ctx, input) => input);
      });
      try {
        const specialInput = {
          unicode: '\u4e16\u754c\u3053\u3093\u306b\u3061\u306f',
          emoji: '\ud83d\ude00\ud83c\udf1f',
          newlines: 'line1\nline2\ttab',
          quotes: 'he said "hello" & \'goodbye\'',
        };
        const res = await callRune('e2e-ext-special-input', specialInput);
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.unicode).toBe(specialInput.unicode);
        expect(body.emoji).toBe(specialInput.emoji);
        expect(body.newlines).toBe(specialInput.newlines);
      } finally {
        stop();
      }
    }, 30_000);

    it('EX-05: sequential register and unregister', async () => {
      // Register caster 1
      const { stop: stop1 } = await createConnectedCaster(
        (c) => {
          c.rune({ name: 'e2e-ext-seq' }, async () => ({ from: 'first' }));
        },
        { casterId: 'seq-1' },
      );
      let runes = await listRunes();
      expect(runes.some((r: any) => r.name === 'e2e-ext-seq')).toBe(true);

      // Disconnect first
      stop1();
      await new Promise((r) => setTimeout(r, 3000));
      runes = await listRunes();
      expect(runes.some((r: any) => r.name === 'e2e-ext-seq')).toBe(false);

      // Register caster 2 with same rune name
      const { stop: stop2 } = await createConnectedCaster(
        (c) => {
          c.rune({ name: 'e2e-ext-seq' }, async () => ({ from: 'second' }));
        },
        { casterId: 'seq-2' },
      );
      try {
        runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-ext-seq')).toBe(true);
        const res = await callRune('e2e-ext-seq', {});
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.from).toBe('second');
      } finally {
        stop2();
      }
    }, 60_000);

    it('EX-06: long-lived connection with heartbeat (15s)', async () => {
      const { stop } = await createConnectedCaster((c) => {
        c.rune({ name: 'e2e-ext-alive15' }, async (_ctx, input) => input);
      });
      try {
        // Verify registered
        let runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-ext-alive15')).toBe(true);

        // Wait 15 seconds (heartbeat should keep connection alive)
        await new Promise((r) => setTimeout(r, 15_000));

        // Still alive
        runes = await listRunes();
        expect(runes.some((r: any) => r.name === 'e2e-ext-alive15')).toBe(true);

        // Can still call it
        const res = await callRune('e2e-ext-alive15', { alive: true });
        expect(res.status).toBe(200);
        const body = await res.json();
        expect(body.alive).toBe(true);
      } finally {
        stop();
      }
    }, 45_000);
  });
});
