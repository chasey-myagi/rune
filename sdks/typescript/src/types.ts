/**
 * Gate routing configuration for exposing a Rune as an HTTP endpoint.
 */
export interface GateConfig {
  /** HTTP path to expose, e.g. "/translate" */
  path: string;
  /** HTTP method, defaults to "POST" */
  method?: string;
}

/**
 * Configuration for declaring a Rune.
 */
export interface RuneConfig {
  /** Unique name of this Rune */
  name: string;
  /** Semantic version string */
  version?: string;
  /** Human-readable description */
  description?: string;
  /** JSON Schema for validating input */
  inputSchema?: Record<string, unknown>;
  /** JSON Schema for validating output */
  outputSchema?: Record<string, unknown>;
  /** Whether this Rune supports streaming responses */
  supportsStream?: boolean;
  /** Gate HTTP endpoint configuration */
  gate?: GateConfig;
  /** Priority for load balancing (higher = preferred) */
  priority?: number;
}

/**
 * A file attachment carried with a request or response.
 */
export interface FileAttachment {
  filename: string;
  data: Buffer;
  mimeType: string;
}

/**
 * Context passed to every Rune handler invocation.
 */
export interface RuneContext {
  /** Name of the Rune being invoked */
  runeName: string;
  /** Unique request ID for this invocation */
  requestId: string;
  /** Arbitrary key-value context from the caller */
  context: Record<string, string>;
  /** AbortSignal that fires when the request is cancelled */
  signal: AbortSignal;
  /** File attachments from the request (if any) */
  attachments?: FileAttachment[];
}

export function getTraceId(ctx: RuneContext): string | undefined {
  return ctx.context["trace_id"];
}

export function getParentRequestId(ctx: RuneContext): string | undefined {
  return ctx.context["parent_request_id"];
}

/**
 * Options for constructing a Caster instance.
 */
export interface CasterOptions {
  /** gRPC endpoint of the Rune Runtime. Default: "localhost:50070" */
  runtime?: string;
  /** API key for authentication */
  key: string;
  /** Unique identifier for this Caster instance. Auto-generated if not provided. */
  casterId?: string;
  /** Heartbeat interval in milliseconds. Default: 10000 */
  heartbeatIntervalMs?: number;
  /** Maximum concurrent request handling. Default: 10 */
  maxConcurrent?: number;
  /** Caster labels for metadata */
  labels?: Record<string, string>;
  /** Reconnection options */
  reconnect?: ReconnectOptions;
}

/**
 * Configuration for exponential backoff reconnection.
 */
export interface ReconnectOptions {
  /** Whether reconnection is enabled. Default: true */
  enabled?: boolean;
  /** Initial delay before first retry in ms. Default: 1000 */
  initialDelayMs?: number;
  /** Maximum delay between retries in ms. Default: 30000 */
  maxDelayMs?: number;
  /** Multiplier for backoff. Default: 2 */
  backoffMultiplier?: number;
}
