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

  /**
   * @internal
   * Constructed by the Caster — not for public use.
   */
  constructor() {
    // Will receive gRPC stream reference once implemented
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
   * Emit a data chunk to the stream.
   * @throws Error if the stream has already ended
   */
  async emit(data: unknown): Promise<void> {
    if (this._ended) {
      throw new Error('Cannot emit after stream has ended');
    }
    this._eventCount++;
    // TODO: serialize data and send via gRPC StreamEvent
    throw new Error('not implemented');
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
    // TODO: send gRPC StreamEnd message
    throw new Error('not implemented');
  }
}
