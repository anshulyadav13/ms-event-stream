/**
 * Replay strategy types
 */
export enum ReplayStrategy {
  /**
   * Replay all events in order
   */
  ALL = 'ALL',

  /**
   * Replay events for specific event types
   */
  BY_EVENT_TYPE = 'BY_EVENT_TYPE',

  /**
   * Replay events for specific time range
   */
  BY_TIME_RANGE = 'BY_TIME_RANGE',

  /**
   * Replay events for specific correlation IDs
   */
  BY_CORRELATION_ID = 'BY_CORRELATION_ID',

  /**
   * Replay events from specific sequence number
   */
  BY_SEQUENCE = 'BY_SEQUENCE',
}

/**
 * Replay mode types
 */
export enum ReplayMode {
  /**
   * Replay events in real-time (respect original timing)
   */
  REAL_TIME = 'REAL_TIME',

  /**
   * Replay events as fast as possible
   */
  FAST = 'FAST',

  /**
   * Replay events with custom delay between events
   */
  CUSTOM_DELAY = 'CUSTOM_DELAY',
}

/**
 * Replay status types
 */
export enum ReplayStatus {
  /**
   * Replay is pending to start
   */
  PENDING = 'PENDING',

  /**
   * Replay is in progress
   */
  IN_PROGRESS = 'IN_PROGRESS',

  /**
   * Replay is paused
   */
  PAUSED = 'PAUSED',

  /**
   * Replay completed successfully
   */
  COMPLETED = 'COMPLETED',

  /**
   * Replay failed
   */
  FAILED = 'FAILED',

  /**
   * Replay was cancelled
   */
  CANCELLED = 'CANCELLED',
}

/**
 * Replay configuration
 */
export interface ReplayConfig {
  /**
   * Enable event replay functionality
   * @default true
   */
  enabled?: boolean;

  /**
   * Maximum number of events to replay in parallel
   * @default 10
   */
  maxConcurrentEvents?: number;

  /**
   * Maximum number of retries for failed replay events
   * @default 3
   */
  maxRetries?: number;

  /**
   * Batch size for replay operations
   * @default 100
   */
  batchSize?: number;

  /**
   * Enable replay progress tracking
   * @default true
   */
  enableProgressTracking?: boolean;

  /**
   * Enable replay monitoring and metrics
   * @default true
   */
  enableMonitoring?: boolean;

  /**
   * Enable replay history
   * @default true
   */
  enableHistory?: boolean;

  /**
   * Maximum number of replay histories to keep
   * @default 100
   */
  maxHistorySize?: number;

  /**
   * Enable replay validation
   * @default true
   */
  enableValidation?: boolean;

  /**
   * Enable replay notifications
   * @default true
   */
  enableNotifications?: boolean;
}

/**
 * Replay options
 */
export interface ReplayOptions {
  /**
   * Replay strategy to use
   * @default ReplayStrategy.ALL
   */
  strategy: ReplayStrategy;

  /**
   * Replay mode to use
   * @default ReplayMode.FAST
   */
  mode: ReplayMode;

  /**
   * Event types to replay (for BY_EVENT_TYPE strategy)
   */
  eventTypes?: string[] | undefined;

  /**
   * Start time for replay (for BY_TIME_RANGE strategy)
   */
  startTime?: Date | undefined;

  /**
   * End time for replay (for BY_TIME_RANGE strategy)
   */
  endTime?: Date | undefined;

  /**
   * Correlation IDs to replay (for BY_CORRELATION_ID strategy)
   */
  correlationIds?: string[] | undefined;

  /**
   * Start sequence number (for BY_SEQUENCE strategy)
   */
  startSequence?: number | undefined;

  /**
   * End sequence number (for BY_SEQUENCE strategy)
   */
  endSequence?: number | undefined;

  /**
   * Custom delay between events in milliseconds (for CUSTOM_DELAY mode)
   */
  delayMs?: number | undefined;

  /**
   * Maximum number of events to replay
   */
  limit?: number | undefined;

  /**
   * Batch size for replay operations
   */
  batchSize?: number | undefined;

  /**
   * Whether to validate events before replay
   * @default true
   */
  validate?: boolean | undefined;

  /**
   * Whether to skip failed events
   * @default false
   */
  skipFailures?: boolean | undefined;

  /**
   * Custom event filter function
   */
  filter?: ((event: ReplayEvent) => boolean | Promise<boolean>) | undefined;

  /**
   * Custom event transform function
   */
  transform?: ((event: ReplayEvent) => ReplayEvent | Promise<ReplayEvent>) | undefined;
}

/**
 * Replay event
 */
export interface ReplayEvent {
  /**
   * Event ID
   */
  eventId: string;

  /**
   * Event type
   */
  eventType: string;

  /**
   * Event payload
   */
  payload: Record<string, any>;

  /**
   * Event metadata
   */
  metadata: {
    /**
     * Original event timestamp
     */
    timestamp: Date;

    /**
     * Original correlation ID
     */
    correlationId?: string;

    /**
     * Original trace ID
     */
    traceId?: string;

    /**
     * Original event version
     */
    version?: string;

    /**
     * Original event sequence number
     */
    sequence?: number;

    /**
     * Original event source
     */
    source?: string;
  };
}

/**
 * Replay progress
 */
export interface ReplayProgress {
  /**
   * Total number of events to replay
   */
  totalEvents: number;

  /**
   * Number of events replayed successfully
   */
  successCount: number;

  /**
   * Number of events that failed
   */
  failureCount: number;

  /**
   * Number of events skipped
   */
  skippedCount: number;

  /**
   * Current progress percentage
   */
  progressPercent: number;

  /**
   * Current replay status
   */
  status: ReplayStatus;

  /**
   * Start time of replay
   */
  startTime: Date;

  /**
   * End time of replay (if completed)
   */
  endTime?: Date;

  /**
   * Error message (if failed)
   */
  error?: string;

  /**
   * Estimated time remaining in milliseconds
   */
  estimatedTimeRemainingMs?: number;

  /**
   * Average events per second
   */
  eventsPerSecond?: number;
}

/**
 * Replay history entry
 */
export interface ReplayHistory {
  /**
   * Unique ID for this replay
   */
  replayId: string;

  /**
   * Replay options used
   */
  options: ReplayOptions;

  /**
   * Final replay progress
   */
  progress: ReplayProgress;

  /**
   * User who initiated the replay
   */
  initiatedBy?: string;

  /**
   * Notes or description for this replay
   */
  description?: string;
}

/**
 * Replay validation result
 */
export interface ReplayValidationResult {
  /**
   * Whether the replay options are valid
   */
  isValid: boolean;

  /**
   * Validation errors
   */
  errors?: string[] | undefined;

  /**
   * Validation warnings
   */
  warnings?: string[] | undefined;
}

/**
 * Replay error
 */
export class ReplayError extends Error {
  constructor(
    message: string,
    public readonly eventId: string,
    public readonly retryable: boolean = true,
    public readonly error?: Error
  ) {
    super(message);
    this.name = 'ReplayError';
  }

  override readonly name: string;
}

/**
 * Replay validation error
 */
export class ReplayValidationError extends Error {
  constructor(
    message: string,
    public readonly errors: string[],
    public readonly warnings?: string[]
  ) {
    super(message);
    this.name = 'ReplayValidationError';
  }

  override readonly name: string;
}

/**
 * Replay not found error
 */
export class ReplayNotFoundError extends Error {
  constructor(
    public override readonly name: string,
    public readonly replayId: string
  ) {
    super(`Replay not found: ${replayId}`);
    this.name = 'ReplayNotFoundError';
  }
}

/**
 * Replay operation not allowed error
 */
export class ReplayOperationNotAllowedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'ReplayOperationNotAllowedError';
  }

  override readonly name: string;
}
