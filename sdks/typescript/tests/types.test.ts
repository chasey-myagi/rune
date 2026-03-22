import { describe, it, expect } from 'vitest';
import type {
  RuneConfig,
  RuneContext,
  GateConfig,
  CasterOptions,
  FileAttachment,
  ReconnectOptions,
} from '../src/types';

describe('RuneConfig', () => {
  it('requires name field', () => {
    const config: RuneConfig = { name: 'translate' };
    expect(config.name).toBe('translate');
  });

  it('has optional version field', () => {
    const config: RuneConfig = { name: 'translate', version: '1.0.0' };
    expect(config.version).toBe('1.0.0');
  });

  it('has optional description field', () => {
    const config: RuneConfig = { name: 'translate', description: 'Translate text' };
    expect(config.description).toBe('Translate text');
  });

  it('has optional inputSchema', () => {
    const config: RuneConfig = {
      name: 'translate',
      inputSchema: { type: 'object', properties: { text: { type: 'string' } } },
    };
    expect(config.inputSchema).toEqual({
      type: 'object',
      properties: { text: { type: 'string' } },
    });
  });

  it('has optional outputSchema', () => {
    const config: RuneConfig = {
      name: 'translate',
      outputSchema: { type: 'string' },
    };
    expect(config.outputSchema).toEqual({ type: 'string' });
  });

  it('has optional supportsStream defaulting to undefined', () => {
    const config: RuneConfig = { name: 'translate' };
    expect(config.supportsStream).toBeUndefined();
  });

  it('has optional gate config', () => {
    const config: RuneConfig = {
      name: 'translate',
      gate: { path: '/translate', method: 'POST' },
    };
    expect(config.gate?.path).toBe('/translate');
    expect(config.gate?.method).toBe('POST');
  });

  it('has optional priority', () => {
    const config: RuneConfig = { name: 'translate', priority: 10 };
    expect(config.priority).toBe(10);
  });
});

describe('RuneContext', () => {
  it('includes runeName', () => {
    const ac = new AbortController();
    const ctx: RuneContext = {
      runeName: 'translate',
      requestId: 'req-1',
      context: {},
      signal: ac.signal,
    };
    expect(ctx.runeName).toBe('translate');
  });

  it('includes requestId', () => {
    const ac = new AbortController();
    const ctx: RuneContext = {
      runeName: 'translate',
      requestId: 'req-abc-123',
      context: {},
      signal: ac.signal,
    };
    expect(ctx.requestId).toBe('req-abc-123');
  });

  it('includes context map', () => {
    const ac = new AbortController();
    const ctx: RuneContext = {
      runeName: 'translate',
      requestId: 'req-1',
      context: { source: 'en', target: 'zh' },
      signal: ac.signal,
    };
    expect(ctx.context).toEqual({ source: 'en', target: 'zh' });
  });

  it('includes abort signal for cancel', () => {
    const ac = new AbortController();
    const ctx: RuneContext = {
      runeName: 'translate',
      requestId: 'req-1',
      context: {},
      signal: ac.signal,
    };
    expect(ctx.signal.aborted).toBe(false);
    ac.abort();
    expect(ctx.signal.aborted).toBe(true);
  });

  it('includes optional attachments', () => {
    const ac = new AbortController();
    const ctx: RuneContext = {
      runeName: 'translate',
      requestId: 'req-1',
      context: {},
      signal: ac.signal,
      attachments: [
        { filename: 'test.txt', data: Buffer.from('hello'), mimeType: 'text/plain' },
      ],
    };
    expect(ctx.attachments).toHaveLength(1);
    expect(ctx.attachments![0].filename).toBe('test.txt');
  });
});

describe('GateConfig', () => {
  it('requires path', () => {
    const gate: GateConfig = { path: '/translate' };
    expect(gate.path).toBe('/translate');
  });

  it('defaults method to undefined (runtime defaults to POST)', () => {
    const gate: GateConfig = { path: '/translate' };
    expect(gate.method).toBeUndefined();
  });

  it('accepts explicit method', () => {
    const gate: GateConfig = { path: '/translate', method: 'GET' };
    expect(gate.method).toBe('GET');
  });
});

describe('CasterOptions', () => {
  it('requires key', () => {
    const opts: CasterOptions = { key: 'rk_test_key' };
    expect(opts.key).toBe('rk_test_key');
  });

  it('has optional runtime field', () => {
    const opts: CasterOptions = { key: 'rk_test_key' };
    expect(opts.runtime).toBeUndefined();
  });

  it('accepts custom runtime', () => {
    const opts: CasterOptions = { key: 'rk_test_key', runtime: 'remote:50070' };
    expect(opts.runtime).toBe('remote:50070');
  });

  it('accepts custom heartbeatIntervalMs', () => {
    const opts: CasterOptions = { key: 'rk_test_key', heartbeatIntervalMs: 5000 };
    expect(opts.heartbeatIntervalMs).toBe(5000);
  });

  it('accepts maxConcurrent', () => {
    const opts: CasterOptions = { key: 'rk_test_key', maxConcurrent: 20 };
    expect(opts.maxConcurrent).toBe(20);
  });

  it('accepts labels', () => {
    const opts: CasterOptions = {
      key: 'rk_test_key',
      labels: { env: 'dev', host: 'mac-studio' },
    };
    expect(opts.labels).toEqual({ env: 'dev', host: 'mac-studio' });
  });
});

describe('FileAttachment', () => {
  it('has filename, data, and mimeType', () => {
    const file: FileAttachment = {
      filename: 'report.pdf',
      data: Buffer.from([0x25, 0x50, 0x44, 0x46]),
      mimeType: 'application/pdf',
    };
    expect(file.filename).toBe('report.pdf');
    expect(file.data).toBeInstanceOf(Buffer);
    expect(file.mimeType).toBe('application/pdf');
  });
});

describe('ReconnectOptions', () => {
  it('all fields are optional', () => {
    const opts: ReconnectOptions = {};
    expect(opts.enabled).toBeUndefined();
    expect(opts.initialDelayMs).toBeUndefined();
    expect(opts.maxDelayMs).toBeUndefined();
    expect(opts.backoffMultiplier).toBeUndefined();
  });

  it('accepts all fields', () => {
    const opts: ReconnectOptions = {
      enabled: false,
      initialDelayMs: 2000,
      maxDelayMs: 60000,
      backoffMultiplier: 3,
    };
    expect(opts.enabled).toBe(false);
    expect(opts.initialDelayMs).toBe(2000);
    expect(opts.maxDelayMs).toBe(60000);
    expect(opts.backoffMultiplier).toBe(3);
  });
});
