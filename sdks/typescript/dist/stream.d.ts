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
export declare class StreamSender {
    private _ended;
    private _eventCount;
    private _sendFn;
    /**
     * @internal
     * Constructed by the Caster — not for public use.
     */
    constructor();
    /**
     * Whether the stream has been ended.
     */
    get ended(): boolean;
    /**
     * Number of events emitted so far.
     */
    get eventCount(): number;
    /**
     * @internal
     * Attach the underlying send function. Called by Caster before dispatching to handler.
     */
    _attach(sendFn: (data: Buffer) => void): void;
    /**
     * Emit a data chunk to the stream.
     * @throws Error if the stream has already ended
     */
    emit(data: unknown): Promise<void>;
    /**
     * End the stream. No more data can be emitted after this.
     * Idempotent — calling end() multiple times is safe.
     */
    end(): Promise<void>;
}
