/**
 * Event status in the outbox pattern
 */
export type OutboxEventStatus = 'pending' | 'sent' | 'failed';

/**
 * Event status in the inbox pattern
 */
export type InboxEventStatus = 'received' | 'processed' | 'failed';

/**
 * Base event interface
 */
export interface BaseEvent {
  /**
   * Unique event identifier
   */
  eventId: string;

  /**
   * Event type/name (e.g., "order.created", "payment.processed")
   */
  eventType: string;

  /**
   * Event payload data
   */
  payload: Record<string, any>;

  /**
   * Timestamp when the event was created
   */
  createdAt: Date;

  /**
   * Optional correlation ID for tracing
   */
  correlationId?: string;

  /**
   * Optional trace ID for distributed tracing
   */
  traceId?: string;

  /**
   * Service that published the event
   */
  sourceService?: string;

  /**
   * Event version for schema evolution
   */
  version?: string;
}

/**
 * Event stored in the outbox table
 */
export interface OutboxEvent extends BaseEvent {
  /**
   * Current status of the event in outbox
   */
  status: OutboxEventStatus;

  /**
   * Number of retry attempts
   */
  retryCount?: number;

  /**
   * Timestamp of last retry attempt
   */
  lastRetryAt?: Date;

  /**
   * Error message if failed
   */
  errorMessage?: string;
}

/**
 * Event stored in the inbox table
 */
export interface InboxEvent extends BaseEvent {
  /**
   * Current status of the event in inbox
   */
  status: InboxEventStatus;

  /**
   * Timestamp when the event was received
   */
  receivedAt: Date;

  /**
   * Timestamp when the event was processed
   */
  processedAt?: Date;

  /**
   * Number of processing attempts
   */
  retryCount?: number;

  /**
   * Error message if processing failed
   */
  errorMessage?: string;
}

/**
 * Event payload that handlers receive
 */
export interface EventPayload<T = any> {
  /**
   * Event metadata
   */
  metadata: {
    eventId: string;
    eventType: string;
    createdAt: Date;
    correlationId?: string;
    traceId?: string;
    sourceService?: string;
    version?: string;
  };

  /**
   * Event data
   */
  data: T;
}

/**
 * Event handler function signature
 */
export type EventHandler<T = any> = (event: EventPayload<T>) => Promise<void> | void;

/**
 * Event handler metadata for registration
 */
export interface EventHandlerMetadata {
  /**
   * Event type this handler processes
   */
  eventType: string;

  /**
   * Handler function
   */
  handler: EventHandler;

  /**
   * Target class instance
   */
  target: any;

  /**
   * Method name on the target class
   */
  methodName: string;

  /**
   * Optional handler options
   */
  options?: {
    /**
     * Priority for handler execution (higher = executed first)
     * @default 0
     */
    priority?: number | undefined;

    /**
     * Whether this handler should run in parallel with others
     * @default true
     */
    parallel?: boolean | undefined;

    /**
     * Custom retry policy for this handler
     */
    retryPolicy?: {
      maxAttempts?: number | undefined;
      baseDelayMs?: number | undefined;
      maxDelayMs?: number | undefined;
      backoffMultiplier?: number | undefined;
      enableJitter?: boolean | undefined;
      jitterPercent?: number | undefined;
    } | undefined;
  } | undefined;
}

/**
 * Event publishing options
 */
export interface PublishOptions {
  /**
   * Correlation ID for tracing
   */
  correlationId?: string;

  /**
   * Trace ID for distributed tracing
   */
  traceId?: string;

  /**
   * Event version
   */
  version?: string;

  /**
   * Custom event ID (if not provided, UUID will be generated)
   */
  eventId?: string;

  /**
   * Delay publishing by specified milliseconds
   */
  delayMs?: number;
}
