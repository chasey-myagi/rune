import { describe, it, expect } from 'vitest';
import { StreamSender } from '../src/stream';

describe('StreamSender', () => {
  it('starts with ended = false', () => {
    const stream = new StreamSender();
    expect(stream.ended).toBe(false);
  });

  it('starts with eventCount = 0', () => {
    const stream = new StreamSender();
    expect(stream.eventCount).toBe(0);
  });

  it('emit() throws not implemented (stub)', async () => {
    const stream = new StreamSender();
    await expect(stream.emit({ token: 'hello' })).rejects.toThrow('not implemented');
  });

  it('emit() increments eventCount before throwing', async () => {
    const stream = new StreamSender();
    try {
      await stream.emit({ token: 'hello' });
    } catch {
      // expected
    }
    expect(stream.eventCount).toBe(1);
  });

  it('emit() after end() throws "Cannot emit after stream has ended"', async () => {
    const stream = new StreamSender();
    // Manually set ended state since end() throws not implemented
    (stream as any)._ended = true;
    await expect(stream.emit({ data: 'test' })).rejects.toThrow(
      'Cannot emit after stream has ended'
    );
  });

  it('end() sets ended = true before throwing not implemented', async () => {
    const stream = new StreamSender();
    try {
      await stream.end();
    } catch {
      // expected: not implemented
    }
    expect(stream.ended).toBe(true);
  });

  it('end() throws not implemented (stub)', async () => {
    const stream = new StreamSender();
    await expect(stream.end()).rejects.toThrow('not implemented');
  });

  it('double end() throws "Stream already ended"', async () => {
    const stream = new StreamSender();
    // Manually set ended state since end() throws not implemented
    (stream as any)._ended = true;
    await expect(stream.end()).rejects.toThrow('Stream already ended');
  });

  it('multiple emit() calls increment eventCount', async () => {
    const stream = new StreamSender();
    for (let i = 0; i < 3; i++) {
      try {
        await stream.emit({ index: i });
      } catch {
        // expected: not implemented
      }
    }
    expect(stream.eventCount).toBe(3);
  });

  it('emit() rejects with Error type', async () => {
    const stream = new StreamSender();
    try {
      await stream.emit('data');
      expect.unreachable('should have thrown');
    } catch (e) {
      expect(e).toBeInstanceOf(Error);
    }
  });
});
