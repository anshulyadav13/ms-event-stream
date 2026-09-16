/**
 * ms-event-stream — Shared Redis Streams publish/consume service
 *
 * Provides a standardized, typed way for nestys microservices to communicate
 * via Redis Streams with consumer groups, crash recovery (XAUTOCLAIM), and
 * dead-letter queues (DLQ).
 *
 * ─────────────────────────────────────────────────────────────────────────
 * STREAM NAMING STANDARD
 * ─────────────────────────────────────────────────────────────────────────
 *
 * All inter-service Redis Streams across nestys microservices MUST follow
 * this naming convention:
 *
 *   Stream:        {consumer-service}:{resource}:{action}:stream
 *   Consumer group: {consumer-service}-{resource}-{action}-workers
 *   DLQ:           {consumer-service}:{resource}:{action}:dlq
 *
 * Where:
 *   - consumer-service: The microservice that consumes the stream
 *     (e.g. "notification")
 *   - resource: The domain resource being acted on
 *     (e.g. "dispatch", "device-token")
 *   - action: What's being done
 *     (e.g. "register", "remove", or omitted for single-action streams)
 *
 * Examples:
 *   notification:dispatch:stream          → notification-workers
 *   notification:device-token:register    → notification-device-token-register-workers
 *   notification:device-token:remove      → notification-device-token-remove-workers
 *
 * When adding a new stream, ALWAYS:
 *   1. Add a method to StreamNames returning { stream, group, dlq }
 *   2. Add a payload interface to stream-payloads.ts
 *   3. Document which MS publishes and which MS consumes
 * ─────────────────────────────────────────────────────────────────────────
 */

export { StreamBusService } from "./stream-bus.service";
export { StreamsModule, STREAM_REDIS } from "./streams.module";
export { IorRedisStreamAdapter } from "./ioredis-stream-adapter";
export { StreamNames, StreamNameSet } from "./stream-names";
export {
  NotificationDispatchPayload,
  NotificationBroadcastPayload,
  DeviceTokenRegisterPayload,
  DeviceTokenRemovePayload,
} from "./stream-payloads";
export {
  IStreamRedis,
  StreamEntry,
  StreamConsumerConfig,
  StreamConsumerHandle,
} from "./stream-redis.interface";
