import { describe, it, expect } from 'vitest';
import { Caster } from '../src/caster';
import type { RuneContext } from '../src/types';
import type { StreamSender } from '../src/stream';

describe('Caster constructor', () => {
  it('accepts runtime URL and key', () => {
    const caster = new Caster({ runtime: 'myhost:9090', key: 'rk_abc' });
    expect(caster.runtime).toBe('myhost:9090');
    expect(caster.key).toBe('rk_abc');
  });

  it('defaults runtime to localhost:50070', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.runtime).toBe('localhost:50070');
  });

  it('stores key from options', () => {
    const caster = new Caster({ key: 'rk_secret' });
    expect(caster.key).toBe('rk_secret');
  });

  it('accepts custom heartbeat interval', () => {
    const caster = new Caster({ key: 'rk_abc', heartbeatIntervalMs: 5000 });
    expect(caster.heartbeatIntervalMs).toBe(5000);
  });

  it('defaults heartbeat interval to 10000ms', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.heartbeatIntervalMs).toBe(10000);
  });

  it('accepts maxConcurrent option', () => {
    const caster = new Caster({ key: 'rk_abc', maxConcurrent: 20 });
    expect(caster.maxConcurrent).toBe(20);
  });

  it('defaults maxConcurrent to 10', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.maxConcurrent).toBe(10);
  });

  it('accepts labels option', () => {
    const caster = new Caster({ key: 'rk_abc', labels: { env: 'prod' } });
    expect(caster.labels).toEqual({ env: 'prod' });
  });

  it('defaults labels to empty object', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.labels).toEqual({});
  });
});

describe('Caster.rune()', () => {
  it('registers a handler for a named rune', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.rune({ name: 'greet' }, async (_ctx, _input) => ({ msg: 'hi' }));
    expect(caster.runeCount).toBe(1);
  });

  it('throws on duplicate rune name', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.rune({ name: 'greet' }, async () => ({}));
    expect(() => {
      caster.rune({ name: 'greet' }, async () => ({}));
    }).toThrow('Rune "greet" is already registered');
  });

  it('handler signature is (ctx, input) => Promise<unknown>', () => {
    const caster = new Caster({ key: 'rk_abc' });
    const handler = async (ctx: RuneContext, input: unknown) => {
      return { name: ctx.runeName, input };
    };
    // Should not throw — type-checks the handler signature
    caster.rune({ name: 'test' }, handler);
    expect(caster.runeCount).toBe(1);
  });

  it('stores inputSchema from config', () => {
    const caster = new Caster({ key: 'rk_abc' });
    const schema = { type: 'object', properties: { text: { type: 'string' } } };
    caster.rune({ name: 'translate', inputSchema: schema }, async () => ({}));
    expect(caster.getRuneConfig('translate')?.inputSchema).toEqual(schema);
  });

  it('stores outputSchema from config', () => {
    const caster = new Caster({ key: 'rk_abc' });
    const schema = { type: 'string' };
    caster.rune({ name: 'translate', outputSchema: schema }, async () => ({}));
    expect(caster.getRuneConfig('translate')?.outputSchema).toEqual(schema);
  });

  it('stores gate path/method config', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.rune(
      { name: 'translate', gate: { path: '/translate', method: 'POST' } },
      async () => ({})
    );
    const config = caster.getRuneConfig('translate');
    expect(config?.gate?.path).toBe('/translate');
    expect(config?.gate?.method).toBe('POST');
  });

  it('marks rune as non-stream', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.rune({ name: 'greet' }, async () => ({}));
    expect(caster.isStreamRune('greet')).toBe(false);
  });
});

describe('Caster.streamRune()', () => {
  it('registers a stream handler', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.streamRune(
      { name: 'chat' },
      async (_ctx: RuneContext, _input: unknown, _stream: StreamSender) => {}
    );
    expect(caster.runeCount).toBe(1);
  });

  it('throws on duplicate rune name', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.streamRune({ name: 'chat' }, async () => {});
    expect(() => {
      caster.streamRune({ name: 'chat' }, async () => {});
    }).toThrow('Rune "chat" is already registered');
  });

  it('throws if name conflicts with unary rune', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.rune({ name: 'greet' }, async () => ({}));
    expect(() => {
      caster.streamRune({ name: 'greet' }, async () => {});
    }).toThrow('Rune "greet" is already registered');
  });

  it('handler signature is (ctx, input, stream) => Promise<void>', () => {
    const caster = new Caster({ key: 'rk_abc' });
    const handler = async (
      ctx: RuneContext,
      input: unknown,
      stream: StreamSender
    ): Promise<void> => {
      void ctx;
      void input;
      void stream;
    };
    caster.streamRune({ name: 'chat' }, handler);
    expect(caster.runeCount).toBe(1);
  });

  it('marks rune as supports_stream', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.streamRune({ name: 'chat' }, async () => {});
    expect(caster.isStreamRune('chat')).toBe(true);
    expect(caster.getRuneConfig('chat')?.supportsStream).toBe(true);
  });
});

describe('Caster.run()', () => {
  it('returns a Promise', () => {
    const caster = new Caster({ key: 'rk_abc' });
    const result = caster.run();
    expect(result).toBeInstanceOf(Promise);
    // Catch the rejection since it throws 'not implemented'
    result.catch(() => {});
  });

  it('rejects with not implemented (stub)', async () => {
    const caster = new Caster({ key: 'rk_abc' });
    await expect(caster.run()).rejects.toThrow('not implemented');
  });
});

describe('Caster connection behavior', () => {
  it('reconnect defaults: enabled = true', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.reconnect.enabled).toBe(true);
  });

  it('reconnect defaults: initialDelayMs = 1000', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.reconnect.initialDelayMs).toBe(1000);
  });

  it('reconnect defaults: maxDelayMs = 30000', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.reconnect.maxDelayMs).toBe(30000);
  });

  it('reconnect defaults: backoffMultiplier = 2', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.reconnect.backoffMultiplier).toBe(2);
  });

  it('custom reconnect options override defaults', () => {
    const caster = new Caster({
      key: 'rk_abc',
      reconnect: { initialDelayMs: 500, maxDelayMs: 10000 },
    });
    expect(caster.reconnect.enabled).toBe(true); // default kept
    expect(caster.reconnect.initialDelayMs).toBe(500);
    expect(caster.reconnect.maxDelayMs).toBe(10000);
    expect(caster.reconnect.backoffMultiplier).toBe(2); // default kept
  });

  it('heartbeat interval is configurable', () => {
    const caster = new Caster({ key: 'rk_abc', heartbeatIntervalMs: 3000 });
    expect(caster.heartbeatIntervalMs).toBe(3000);
  });
});

describe('Caster multiple rune registration', () => {
  it('can register multiple different runes', () => {
    const caster = new Caster({ key: 'rk_abc' });
    caster.rune({ name: 'greet' }, async () => ({}));
    caster.rune({ name: 'translate' }, async () => ({}));
    caster.streamRune({ name: 'chat' }, async () => {});
    expect(caster.runeCount).toBe(3);
  });

  it('getRuneConfig returns undefined for unregistered rune', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.getRuneConfig('nonexistent')).toBeUndefined();
  });

  it('isStreamRune returns false for unregistered rune', () => {
    const caster = new Caster({ key: 'rk_abc' });
    expect(caster.isStreamRune('nonexistent')).toBe(false);
  });
});
