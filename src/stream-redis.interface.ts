/**
 * Interface that each microservice's RedisService must implement for the
 * StreamBusService to use. This decouples the shared package from any
 * particular Redis client library or configuration.
 *
 * All stream operations use the EVENT Redis client (the inter-service
 * communication Redis instance), not the main/cache Redis client.
 */

/** A single entry read from a Redis Stream via XREADGROUP or XAUTOCLAIM. */
export interface StreamEntry {
  /** Auto-generated stream entry ID (e.g. "1234567890-0"). */
  id: string;
  /** Flat key-value pairs from the stream entry (all values are strings). */
  fields: Record<string, string>;
}

/**
 * The subset of Redis Streams operations that StreamBusService needs.
 * Each microservice's RedisService must implement this interface and
 * provide it via the `STREAM_REDIS` injection token.
 */
export interface IStreamRedis {
  /**
   * Appends an entry to a Redis Stream (XADD).
   * @param stream - Stream key
   * @param fields - Flat key-value pairs (all values must be strings)
   * @param maxlen - Optional approximate cap on stream length (MAXLEN ~ N)
   * @returns The auto-generated stream entry ID
   */
  xadd(
    stream: string,
    fields: Record<string, string>,
    maxlen?: number,
  ): Promise<string>;

  /**
   * Creates a consumer group on a stream (XGROUP CREATE).
   * If the group already exists (BUSYGROUP), this is a no-op.
   * @param stream - Stream key
   * @param group - Consumer group name
   * @param startId - Where to start reading: "$" (new only) or "0" (all existing)
   * @param mkstream - If true, create the stream if it doesn't exist
   */
  xgroupCreate(
    stream: string,
    group: string,
    startId: string,
    mkstream?: boolean,
  ): Promise<void>;

  /**
   * Reads entries from a stream via a consumer group (XREADGROUP).
   * @param stream - Stream key
   * @param group - Consumer group name
   * @param consumer - Consumer name (unique per instance)
   * @param count - Max entries to read
   * @param blockMs - Long-poll block duration in milliseconds
   * @returns Array of { id, fields } entries, or empty array if none available
   */
  xreadGroup(
    stream: string,
    group: string,
    consumer: string,
    count: number,
    blockMs: number,
  ): Promise<StreamEntry[]>;

  /**
   * Acknowledges that a message has been processed (XACK).
   * Removes the message from the consumer group's Pending Entries List (PEL).
   * @param stream - Stream key
   * @param group - Consumer group name
   * @param ids - Stream entry IDs to acknowledge
   * @returns Number of entries acknowledged
   */
  xack(stream: string, group: string, ...ids: string[]): Promise<number>;

  /**
   * Atomically claims idle messages from the PEL (XAUTOCLAIM).
   * Used by the janitor sweep to recover messages from crashed/stopped consumers.
   * @param stream - Stream key
   * @param group - Consumer group name
   * @param consumer - Consumer name claiming the messages
   * @param minIdleMs - Minimum idle time (ms) for a message to be claimed
   * @param startId - Cursor start position ("0-0" for beginning of PEL)
   * @param count - Max entries to claim
   * @returns { nextCursor, entries } where nextCursor is the cursor for the next call
   */
  xautoclaim(
    stream: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    startId: string,
    count: number,
  ): Promise<{ nextCursor: string; entries: StreamEntry[] }>;

  /**
   * Returns pending message details for a specific ID range (XPENDING).
   * Used to check delivery count for poison-message detection.
   * @param stream - Stream key
   * @param group - Consumer group name
   * @param start - Start ID (use entry ID for single message)
   * @param end - End ID (use entry ID for single message)
   * @param count - Max results
   * @returns Array of [id, consumer, idleMs, deliveryCount] tuples
   */
  xpending(
    stream: string,
    group: string,
    start: string,
    end: string,
    count: number,
  ): Promise<Array<[string, string, number, number]>>;

  /**
   * Returns the length of a stream (total entries).
   * @param stream - Stream key
   * @returns Number of entries in the stream
   */
  xlen(stream: string): Promise<number>;
}

/**
 * Configuration for starting a stream consumer via StreamBusService.
 */
export interface StreamConsumerConfig<T> {
  /** The stream to consume from. Use StreamNames to get the standard name. */
  stream: string;
  /** Consumer group name. Use StreamNames to get the standard group. */
  group: string;
  /** Dead-letter queue stream. Poison messages exceeding maxDeliveryCount are moved here. */
  dlq?: string;
  /**
   * Handler called for each stream entry. If it throws, the message is NOT
   * ACK'd — it stays in the PEL and is retried by the reclaim sweep. Errors
   * propagate as unhandled rejections to the global exception handler.
   * @param payload - Deserialized typed payload
   * @param entryId - The stream entry ID
   */
  handler: (payload: T, entryId: string) => Promise<void>;
  /** Unique consumer identifier. Auto-generated if not provided. */
  consumerId?: string;
  /** Poll interval in milliseconds. Default: 1000 */
  pollIntervalMs?: number;
  /** Max entries per poll batch. Default: 20 */
  batchSize?: number;
  /** Long-poll block duration in milliseconds. Default: 500 */
  blockMs?: number;
  /** Reclaim sweep interval in milliseconds. Default: 30000 */
  reclaimIntervalMs?: number;
  /** Min idle time (ms) before a message is considered stuck. Default: 60000 */
  minIdleMs?: number;
  /** Max delivery attempts before moving to DLQ. Default: 5 */
  maxDeliveryCount?: number;
  /** Approximate max stream length (MAXLEN ~ N). Default: 100000 */
  streamMaxLen?: number;
}

/**
 * Handle returned by startConsumer(), used to stop the consumer.
 */
export interface StreamConsumerHandle {
  /** The stream being consumed. */
  stream: string;
  /** Stops the consumer's poll and reclaim timers. */
  stop(): void;
}
