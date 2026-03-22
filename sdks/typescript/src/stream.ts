/**
 * StreamSender allows a stream handler to emit data chunks back to the caller.
 *
 * Usage within a streamRune handler:
 *   async (ctx, input, stream) => {
 *     await stream.emit({ token: "hello" });
 *     await stream.emit({ token: "world" });
 *     // stream.end() is called automatically after handler returns
 *   }
 */
export class StreamSender {
  private _ended = false;
  private _eventCount = 0;
  private _sendFn: ((data: Buffer) => void) | null = null;

  /**
   * @internal
   * Constructed by the Caster — not for public use.
   */
  constructor() {
    // Will receive gRPC stream reference via _attach()
  }

  /**
   * Whether the stream has been ended.
   */
  get ended(): boolean {
    return this._ended;
  }

  /**
   * Number of events emitted so far.
   */
  get eventCount(): number {
    return this._eventCount;
  }

  /**
   * @internal
   * Attach the underlying send function. Called by Caster before dispatching to handler.
   */
  _attach(sendFn: (data: Buffer) => void): void {
    this._sendFn = sendFn;
  }

  /**
   * Emit a data chunk to the stream.
   * @throws Error if the stream has already ended
   */
  async emit(data: unknown): Promise<void> {
    if (this._ended) {
      throw new Error('Cannot emit after stream has ended');
    }
    this._eventCount++;

    if (!this._sendFn) {
      throw new Error('not implemented');
    }

    const buf =
      typeof data === 'string'
        ? Buffer.from(data)
        : Buffer.isBuffer(data)
          ? data
          : Buffer.from(JSON.stringify(data));
    this._sendFn(buf);
  }

  /**
   * End the stream. No more data can be emitted after this.
   * @throws Error if the stream has already ended
   */
  async end(): Promise<void> {
    if (this._ended) {
      throw new Error('Stream already ended');
    }
    this._ended = true;

    if (!this._sendFn) {
      throw new Error('not implemented');
    }
  }
}
