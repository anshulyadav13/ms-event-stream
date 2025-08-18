/**
 * ms-event-stream - A NestJS package for reliable event-driven microservices
 * 
 * This package provides a plug-and-play event system with:
 * - Outbox/Inbox patterns for reliability
 * - Redis Streams for high-performance messaging
 * - Event deduplication and retry mechanisms
 * - Dead Letter Queue (DLQ) support
 * - Metrics and observability
 * - Horizontal scaling support
 */

// Core module and service exports
export { EventStreamModule, EVENT_STREAM_CONFIG } from './event-stream.module';
export { EventStreamService } from './event-stream.service';

// Adapter exports
export { DatabaseAdapter } from './adapters/db.adapter';
export { RedisAdapter } from './adapters/redis.adapter';

// Manager exports
export { OutboxManager } from './managers/outbox.manager';
export { InboxManager } from './managers/inbox.manager';
export { RetryManager } from './managers/retry.manager';
export { DLQManager } from './managers/dlq.manager';
export { ArchiveManager } from './managers/archive.manager';

// Jobs
export { ArchiverJob } from './jobs/archiver.job';

// Observability exports (Phase 11)
export { MetricsCollector } from './observability/metrics.collector';
export { HealthChecker } from './observability/health.checker';

// Consumer and registry exports (Phase 6)
export { EventConsumer } from './consumers/event.consumer';
export { HandlerRegistry } from './registries/handler.registry';

// Decorator and discovery exports (Phase 7)
export { OnEvent, OnEvents, OnEventIf, createEventTypes } from './decorators/on-event.decorator';
export { EventHandlerDiscovery } from './discovery/event-handler.discovery';

// Retry and policy exports (Phase 8)
export { 
  ErrorType,
  RetryPolicy,
  RetryAttemptResult,
  RetryOperationResult,
  CircuitBreakerState,
  CircuitBreakerConfig,
} from './managers/retry.manager';
export {
  RetryPolicyBuilder,
  RetryPolicies,
  RetryCondition,
  RetryConditions,
  RetryContext,
  RetryExecutionOptions,
} from './policies/retry.policy';

// DLQ exports (Phase 9)
export type {
  DLQEventData,
  DLQEvent,
  DLQStats,
  DLQReplayOptions,
  DLQReplayResult,
  DLQPurgeOptions,
  DLQPurgeResult,
} from './managers/dlq.manager';

// Archive exports (Phase 10)
export type {
  ArchiveConfig,
  ArchiveResult,
  ArchiveStats,
  ArchiveQueryOptions,
  ArchivedEvent,
} from './managers/archive.manager';

// Archiver Job exports (Phase 10)
export type {
  ArchiverJobConfig,
  JobExecutionResult,
  JobStats,
} from './jobs/archiver.job';

// Observability exports (Phase 11)
export type {
  MetricsConfig,
  MetricType,
  BaseMetric,
  CounterMetric,
  GaugeMetric,
  HistogramMetric,
  SummaryMetric,
  Metric,
  MetricsSnapshot,
  PerformanceMetrics,
  BusinessMetrics,
  SystemMetrics,
} from './observability/metrics.collector';

export type {
  HealthCheckConfig,
  HealthStatus,
  HealthCheckResult,
  ComponentHealth,
  SystemHealth,
  HealthCheckContext,
  HealthEndpointResponse,
} from './observability/health.checker';

// Consumer and handler interfaces
export type {
  ConsumerOptions,
  ConsumerHealth,
  ConsumerStats,
  EventProcessingResult,
} from './consumers/event.consumer';

export type {
  HandlerExecutionResult,
  HandlerExecutionContext,
  HandlerStats,
} from './registries/handler.registry';

// Decorator and discovery interfaces (Phase 7)
export type {
  OnEventOptions,
  EventHandlerDecoratorMetadata,
  TypedEventPayload,
} from './decorators/on-event.decorator';

export type {
  DiscoveryStats,
  HandlerDiscoveryConfig,
} from './discovery/event-handler.discovery';

// Schema exports
export {
  EventStatus,
  TABLE_NAMES,
  OUTBOX_EVENTS_SCHEMA,
  INBOX_EVENTS_SCHEMA,
  OUTBOX_EVENTS_ARCHIVED_SCHEMA,
  INBOX_EVENTS_ARCHIVED_SCHEMA,
} from './schemas/database.schema';

// Interface exports
export {
  // Configuration interfaces
  EventStreamConfig,
  ResolvedEventStreamConfig,
  Environment,
  
  // Event interfaces
  BaseEvent,
  OutboxEvent,
  InboxEvent,
  EventPayload,
  EventHandler,
  EventHandlerMetadata,
  PublishOptions,
  
  // Status enums
  OutboxEventStatus,
  InboxEventStatus,
} from './interfaces';

// Re-export all interfaces for convenience
export * from './interfaces/config.interface';
export * from './interfaces/event.interface';

/**
 * Package version
 */
export const VERSION = '1.0.0';

/**
 * Package name
 */
export const PACKAGE_NAME = 'ms-event-stream';

// CLI export
export { program as CLI } from './cli/main';
