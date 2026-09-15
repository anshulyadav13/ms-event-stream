import { Injectable, Logger } from "@nestjs/common";
import { randomUUID } from "crypto";
import {
  IStreamRedis,
  StreamEntry,
  StreamConsumerConfig,
  StreamConsumerHandle,
} from "./stream-redis.interface";

/**
 * StreamBusService — standardized Redis Streams publish/consume for nestys.
 *
 * This service provides a single, consistent API for all microservices to
 * communicate via Redis Streams. It handles:
 *
 *  - **Publishing**: serializing typed payloads to flat string fields and
 *    appending to a stream via XADD.
 *  - **Consuming**: creating consumer groups, polling via XREADGROUP,
 *    acknowledging via XACK, and recovering crashed consumers via
 *    XAUTOCLAIM.
 *  - **Dead-letter queues**: poison messages that exceed maxDeliveryCount
 *    are moved to a DLQ stream instead of being retried forever.
 *  - **Crash recovery**: unacked messages stay in the PEL and are
 *    reclaimed by the janitor sweep on the next interval.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * USAGE — Publisher (e.g. user MS, auth MS):
 *
 *   constructor(private streamBus: StreamBusService) {}
 *
 *   async sendNotification() {
 *     const names = StreamNames.notificationDispatch();
 *     await this.streamBus.publish(names.stream, {
 *       userId: 10,
 *       channel: "PUSH",
 *       idempotencyKey: "key-123",
 *       title: "Hello",
 *     });
 *   }
 *
 * ─────────────────────────────────────────────────────────────────────────
 * USAGE — Consumer (e.g. notification MS):
 *
 *   async onModuleInit() {
 *     const names = StreamNames.notificationDispatch();
 *     this.handle = await this.streamBus.startConsumer<NotificationDispatchPayload>({
 *       stream: names.stream,
 *       group: names.group,
 *       dlq: names.dlq,
 *       handler: async (payload, entryId) => {
 *         await this.dispatchService.dispatch(payload);
 *       },
 *     });
 *   }
 *
 *   onModuleDestroy() { this.handle?.stop(); }
 *
 * ─────────────────────────────────────────────────────────────────────────
 * ERROR HANDLING:
 *
 * The handler is called WITHOUT a try/catch. If it throws:
 *   1. The message is NOT ACK'd — it stays in the PEL.
 *   2. The error propagates as an unhandled rejection to the process-level
 *      global exception handler (GlobalExceptionHandlerService), which logs
 *      it and sends an exception email.
 *   3. The remaining entries in the batch also stay in the PEL.
 *   4. The reclaim sweep retries the message on the next interval.
 *   5. After maxDeliveryCount attempts, the message is moved to the DLQ.
 *
 * This matches the existing notification MS pattern where unexpected errors
 * are never silently swallowed.
 * ─────────────────────────────────────────────────────────────────────────
 */
@Injectable()
export class StreamBusService {
  private readonly logger = new Logger(StreamBusService.name);
  private readonly consumers = new Map<string, StreamConsumerHandleInternal>();

  constructor(private readonly redis: IStreamRedis) {}

  // ─────────────────────────────────────────────────────────────────────
  // PUBLISH
  // ─────────────────────────────────────────────────────────────────────

  /**
   * Publishes a typed payload to a Redis Stream via XADD.
   *
   * The payload is serialized to flat string fields:
   *  - Primitive values (string, number, boolean) are converted to strings.
   *  - Complex values (objects, arrays) are JSON.stringify'd.
   *  - `undefined` fields are omitted.
   *
   * @param stream - Stream key (use StreamNames to get the standard name)
   * @param payload - Typed payload object
   * @param options.maxlen - Optional approximate cap on stream length
   * @returns The auto-generated stream entry ID
   */
  async publish<T>(
    stream: string,
    payload: T,
    options?: { maxlen?: number },
  ): Promise<string> {
    const fields = this.serializePayload(payload as Record<string, unknown>);
    const maxlen = options?.maxlen ?? 100_000;
    const id = await this.redis.xadd(stream, fields, maxlen);
    this.logger.debug(
      `Published to stream=${stream} entryId=${id} fields=${Object.keys(fields).join(",")}`,
    );
    return id;
  }

  // ─────────────────────────────────────────────────────────────────────
  // CONSUME
  // ─────────────────────────────────────────────────────────────────────

  /**
   * Starts a consumer for a Redis Stream.
   *
   * Creates the consumer group (if not already existing) with startId "0" so
   * that messages published while the consumer was down are processed on
   * first startup. Then starts two timers:
   *
   *  1. **Poll loop**: reads up to `batchSize` entries via XREADGROUP,
   *     calls the handler for each, and ACKs on success.
   *  2. **Reclaim sweep**: claims idle (unacked) messages via XAUTOCLAIM,
   *     checks delivery count, and moves poison messages to the DLQ.
   *
   * The handler is called WITHOUT a try/catch — see the class-level docs
   * for error handling behavior.
   *
   * @param config - Consumer configuration (stream, group, handler, etc.)
   * @returns A handle that can be used to stop the consumer
   */
  async startConsumer<T>(
    config: StreamConsumerConfig<T>,
  ): Promise<StreamConsumerHandle> {
    const {
      stream,
      group,
      dlq,
      handler,
      consumerId = randomUUID(),
      pollIntervalMs = 1000,
      batchSize = 20,
      blockMs = 500,
      reclaimIntervalMs = 30_000,
      minIdleMs = 60_000,
      maxDeliveryCount = 5,
    } = config;

    // Create the consumer group. startId "0" means "process all existing
    // entries from the beginning" — so messages published while the
    // consumer was down are picked up on first startup. On subsequent
    // restarts, the group already exists (BUSYGROUP is swallowed by
    // xgroupCreate) and the group's last-delivered-id is preserved.
    await this.redis.xgroupCreate(stream, group, "0");

    let running = false;
    let pollTimer: NodeJS.Timeout | null = null;
    let reclaimTimer: NodeJS.Timeout | null = null;

    const poll = async () => {
      if (running) return;
      running = true;
      try {
        const entries = await this.redis.xreadGroup(
          stream,
          group,
          consumerId,
          batchSize,
          blockMs,
        );

        // Process all entries in the batch in parallel. If an entry's
        // handler succeeds, it is ACK'd; if it throws, the entry stays
        // un-ACK'd in the PEL and is retried by the reclaim sweep. This
        // means a slow or failing entry never blocks the ACK of a
        // successful one (unlike the previous sequential loop).
        const results = await Promise.allSettled(
          entries.map(async (entry) => {
            const payload = this.deserializePayload<T>(entry.fields);
            await handler(payload, entry.id);
            return entry.id;
          }),
        );

        // Batch-ACK all successful entries in a single Redis call
        const successfulIds = results
          .filter(
            (r): r is PromiseFulfilledResult<string> =>
              r.status === "fulfilled",
          )
          .map((r) => r.value);
        if (successfulIds.length > 0) {
          await this.redis.xack(stream, group, ...successfulIds);
        }

        // Propagate the first rejection so the global exception handler
        // logs it and sends an alert email (same semantics as before).
        const firstRejection = results.find(
          (r): r is PromiseRejectedResult => r.status === "rejected",
        );
        if (firstRejection) {
          throw firstRejection.reason;
        }
      } finally {
        running = false;
      }
    };

    const reclaimStuckMessages = async () => {
      if (!dlq) return;

      let cursor = "0-0";
      let reclaimed = 0;
      let deadLettered = 0;

      while (true) {
        const result = await this.redis.xautoclaim(
          stream,
          group,
          consumerId,
          minIdleMs,
          cursor,
          batchSize,
        );

        if (result.entries.length === 0 && result.nextCursor === "0-0") break;

        // Check delivery counts for all reclaimed entries in parallel
        const deliveryCounts = await Promise.all(
          result.entries.map(async (entry) => {
            const pending = await this.redis.xpending(
              stream,
              group,
              entry.id,
              entry.id,
              1,
            );
            return {
              entry,
              deliveryCount: pending.length > 0 ? pending[0][3] : 1,
            };
          }),
        );

        // Move poison messages to DLQ (sequential — typically very few)
        const poisonEntries = deliveryCounts.filter(
          (d) => d.deliveryCount > maxDeliveryCount,
        );
        for (const { entry, deliveryCount } of poisonEntries) {
          await this.redis.xadd(dlq, {
            originalId: entry.id,
            ...entry.fields,
            deliveryCount: String(deliveryCount),
            deadLetteredAt: new Date().toISOString(),
          });
          await this.redis.xack(stream, group, entry.id);
          deadLettered++;
          this.logger.warn(
            `Moved poison message ${entry.id} to DLQ (deliveryCount=${deliveryCount}) stream=${stream}`,
          );
        }

        // Reprocess non-poison entries in parallel — no try/catch, errors
        // propagate to the global handler. Failed entries stay in the PEL
        // and are retried on the next sweep.
        const reprocessable = deliveryCounts.filter(
          (d) => d.deliveryCount <= maxDeliveryCount,
        );
        const reprocessResults = await Promise.allSettled(
          reprocessable.map(async ({ entry }) => {
            const payload = this.deserializePayload<T>(entry.fields);
            await handler(payload, entry.id);
            return entry.id;
          }),
        );

        // Batch-ACK all successfully reprocessed entries
        const reprocessedIds = reprocessResults
          .filter(
            (r): r is PromiseFulfilledResult<string> =>
              r.status === "fulfilled",
          )
          .map((r) => r.value);
        if (reprocessedIds.length > 0) {
          await this.redis.xack(stream, group, ...reprocessedIds);
        }
        reclaimed += reprocessedIds.length;

        if (result.nextCursor === "0-0") break;
        cursor = result.nextCursor;
      }

      if (reclaimed > 0 || deadLettered > 0) {
        this.logger.log(
          `Reclaim sweep complete: stream=${stream} reclaimed=${reclaimed} deadLettered=${deadLettered}`,
        );
      }
    };

    pollTimer = setInterval(() => {
      void poll();
    }, pollIntervalMs);

    reclaimTimer = setInterval(() => {
      void reclaimStuckMessages();
    }, reclaimIntervalMs);

    const internal: StreamConsumerHandleInternal = {
      stream,
      consumerId,
      stop: () => {
        if (pollTimer) clearInterval(pollTimer);
        if (reclaimTimer) clearInterval(reclaimTimer);
        pollTimer = null;
        reclaimTimer = null;
        this.consumers.delete(stream);
      },
    };

    this.consumers.set(stream, internal);

    this.logger.log(
      `Stream consumer started: stream=${stream} group=${group} consumerId=${consumerId} pollIntervalMs=${pollIntervalMs} batchSize=${batchSize} maxDeliveryCount=${maxDeliveryCount}`,
    );

    return internal;
  }

  /**
   * Stops a running consumer by stream name.
   * @param stream - The stream key the consumer was started for
   */
  stopConsumer(stream: string): void {
    const consumer = this.consumers.get(stream);
    if (consumer) {
      consumer.stop();
    }
  }

  /**
   * Stops all running consumers. Called on module destroy.
   */
  stopAllConsumers(): void {
    for (const consumer of this.consumers.values()) {
      consumer.stop();
    }
  }

  // ─────────────────────────────────────────────────────────────────────
  // SERIALIZATION
  // ─────────────────────────────────────────────────────────────────────

  /**
   * Serializes a typed payload to flat string fields for XADD.
   *
   * - Primitives (string, number, boolean) → String(value)
   * - Objects/arrays → JSON.stringify
   * - undefined → omitted
   */
  private serializePayload(
    payload: Record<string, unknown>,
  ): Record<string, string> {
    const fields: Record<string, string> = {};
    for (const [key, value] of Object.entries(payload)) {
      if (value === undefined || value === null) continue;
      if (typeof value === "object") {
        fields[key] = JSON.stringify(value);
      } else {
        fields[key] = String(value);
      }
    }
    return fields;
  }

  /**
   * Deserializes flat string fields from a stream entry back to a typed
   * payload.
   *
   * Heuristic for JSON fields: if a value starts with "{" or "[", attempt
   * JSON.parse. If parsing fails, keep the raw string.
   */
  private deserializePayload<T>(
    fields: Record<string, string>,
  ): T {
    const payload: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(fields)) {
      if (
        (value.startsWith("{") && value.endsWith("}")) ||
        (value.startsWith("[") && value.endsWith("]"))
      ) {
        try {
          payload[key] = JSON.parse(value);
        } catch {
          payload[key] = value;
        }
      } else {
        payload[key] = value;
      }
    }
    return payload as T;
  }
}

/**
 * Internal consumer handle with the consumer ID for logging.
 */
interface StreamConsumerHandleInternal extends StreamConsumerHandle {
  consumerId: string;
}
