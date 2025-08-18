import { SchemaCompatibilityLevel, SchemaEvolutionStrategy } from '../schema/interfaces';

/**
 * Environment types supported by the event system
 */
export type Environment = 'development' | 'staging' | 'production';

/**
 * Configuration interface for the Event Stream module
 */
export interface EventStreamConfig {
  /**
   * Unique service name - used for stream naming and consumer groups
   * @example "order-service", "payment-service"
   */
  serviceName: string;

  /**
   * Database connection URL
   * @example "postgresql://user:pass@localhost:5432/orders"
   */
  dbUrl: string;

  /**
   * Redis connection URL
   * @example "redis://localhost:6379"
   */
  redisUrl: string;

  /**
   * Environment the service is running in
   * @default "development"
   */
  env?: Environment;

  /**
   * Optional database configuration
   */
  database?: {
    /**
     * Maximum number of connections in the pool
     * @default 10
     */
    maxConnections?: number;

    /**
     * Connection timeout in milliseconds
     * @default 5000
     */
    connectionTimeoutMs?: number;

    /**
     * Idle timeout in milliseconds
     * @default 30000
     */
    idleTimeoutMs?: number;
  };

  /**
   * Optional Redis configuration
   */
  redis?: {
    /**
     * Maximum number of retries for Redis operations
     * @default 3
     */
    retryAttempts?: number;

    /**
     * Delay between retry attempts in milliseconds
     * @default 1000
     */
    retryDelayMs?: number;

    /**
     * Connection timeout in milliseconds
     * @default 5000
     */
    connectionTimeoutMs?: number;

    /**
     * Maximum length of Redis streams (for automatic trimming)
     * @default 100000
     */
    maxStreamLength?: number;
  };

  /**
   * Event processing configuration
   */
  events?: {
    /**
     * Maximum number of events to process in parallel
     * @default 10
     */
    maxConcurrentEvents?: number;

    /**
     * Timeout for event processing in milliseconds
     * @default 30000
     */
    processingTimeoutMs?: number;

    /**
     * Enable event deduplication (recommended for production)
     * @default true
     */
    enableDeduplication?: boolean;
  };

  /**
   * Outbox pattern configuration
   */
  outbox?: {
    /**
     * Maximum number of retry attempts for failed events
     * @default 3
     */
    maxRetries?: number;

    /**
     * Number of days to keep sent events before cleanup
     * @default 7
     */
    retentionDays?: number;

    /**
     * Enable automatic cleanup of old sent events
     * @default true
     */
    enableCleanup?: boolean;

    /**
     * Interval for cleanup job in milliseconds
     * @default 86400000 (24 hours)
     */
    cleanupIntervalMs?: number;

    /**
     * Retry configuration
     */
    retry?: {
      /**
       * Base delay in milliseconds for exponential backoff
       * @default 1000
       */
      baseDelayMs?: number;

      /**
       * Maximum delay in milliseconds
       * @default 30000
       */
      maxDelayMs?: number;

      /**
       * Multiplier for exponential backoff
       * @default 2
       */
      backoffMultiplier?: number;

      /**
       * Enable jitter to prevent thundering herd
       * @default true
       */
      enableJitter?: boolean;

      /**
       * Jitter percentage (0-1)
       * @default 0.1
       */
      jitterPercent?: number;
    };
  };

  /**
   * Inbox pattern configuration
   */
  inbox?: {
    /**
     * Maximum number of retry attempts for failed events
     * @default 3
     */
    maxRetries?: number;

    /**
     * Number of days to keep processed events before cleanup
     * @default 7
     */
    retentionDays?: number;

    /**
     * Enable automatic cleanup of old processed events
     * @default true
     */
    enableCleanup?: boolean;

    /**
     * Interval for cleanup job in milliseconds
     * @default 86400000 (24 hours)
     */
    cleanupIntervalMs?: number;

    /**
     * Retry configuration
     */
    retry?: {
      /**
       * Base delay in milliseconds for exponential backoff
       * @default 1000
       */
      baseDelayMs?: number;

      /**
       * Maximum delay in milliseconds
       * @default 30000
       */
      maxDelayMs?: number;

      /**
       * Multiplier for exponential backoff
       * @default 2
       */
      backoffMultiplier?: number;

      /**
       * Enable jitter to prevent thundering herd
       * @default true
       */
      enableJitter?: boolean;

      /**
       * Jitter percentage (0-1)
       * @default 0.1
       */
      jitterPercent?: number;
    };
  };

  /**
   * Consumer configuration
   */
  consumer?: {
    /**
     * Number of messages to read in each batch
     * @default 10
     */
    batchSize?: number;

    /**
     * Time to block waiting for new messages in milliseconds
     * @default 1000
     */
    blockTimeMs?: number;

    /**
     * Interval between polling attempts in milliseconds
     * @default 5000
     */
    pollIntervalMs?: number;

    /**
     * Maximum number of retry attempts for failed message processing
     * @default 3
     */
    maxRetries?: number;

    /**
     * Timeout for graceful shutdown in milliseconds
     * @default 30000
     */
    shutdownTimeoutMs?: number;

    /**
     * Enable consumer health checks
     * @default true
     */
    enableHealthChecks?: boolean;

    /**
     * Maximum number of concurrent consumers
     * @default 1
     */
    maxConcurrentConsumers?: number;

    /**
     * Auto-start consumers on service initialization
     * @default true
     */
    autoStart?: boolean;
  };

  /**
   * Handler discovery configuration
   */
  discovery?: {
    /**
     * Whether to enable automatic discovery of @OnEvent handlers
     * @default true
     */
    enabled?: boolean;

    /**
     * Packages/modules to include in discovery
     * If empty, scans all modules
     */
    includeModules?: string[];

    /**
     * Packages/modules to exclude from discovery
     */
    excludeModules?: string[];

    /**
     * Whether to validate handler methods before registration
     * @default true
     */
    validateHandlers?: boolean;

    /**
     * Whether to throw errors on discovery failures
     * @default false (log errors but continue)
     */
    throwOnError?: boolean;

    /**
     * Whether to log discovery process details
     * @default true
     */
    enableLogging?: boolean;
  };

  /**
   * Circuit breaker configuration
   */
  circuitBreaker?: {
    /**
     * Enable circuit breaker pattern
     * @default true
     */
    enabled?: boolean;

    /**
     * Failure threshold to open circuit
     * @default 5
     */
    failureThreshold?: number;

    /**
     * Recovery timeout in milliseconds
     * @default 60000
     */
    recoveryTimeoutMs?: number;

    /**
     * Time window for counting failures in milliseconds
     * @default 60000
     */
    timeWindowMs?: number;

    /**
     * Minimum requests before circuit can open
     * @default 10
     */
    minimumRequestThreshold?: number;
  };

  /**
   * Global retry configuration (overrides component-specific settings)
   */
  retry?: {
    /**
     * Enable retry functionality globally
     * @default true
     */
    enabled?: boolean;

    /**
     * Default maximum retry attempts
     * @default 3
     */
    defaultMaxAttempts?: number;

    /**
     * Default base delay in milliseconds
     * @default 1000
     */
    defaultBaseDelayMs?: number;

    /**
     * Default maximum delay in milliseconds
     * @default 30000
     */
    defaultMaxDelayMs?: number;

    /**
     * Default backoff multiplier
     * @default 2
     */
    defaultBackoffMultiplier?: number;

    /**
     * Enable jitter by default
     * @default true
     */
    defaultEnableJitter?: boolean;

    /**
     * Default jitter percentage
     * @default 0.1
     */
    defaultJitterPercent?: number;
  };

  /**
   * Dead Letter Queue configuration
   */
  dlq?: {
    /**
     * Enable DLQ functionality
     * @default true
     */
    enabled?: boolean;

    /**
     * Maximum length of DLQ stream (for automatic trimming)
     * @default 10000
     */
    maxStreamLength?: number;

    /**
     * Number of days to keep events in DLQ before cleanup
     * @default 30
     */
    retentionDays?: number;

    /**
     * Enable automatic cleanup of old DLQ events
     * @default true
     */
    enableCleanup?: boolean;

    /**
     * Interval for cleanup job in milliseconds
     * @default 86400000 (24 hours)
     */
    cleanupIntervalMs?: number;

    /**
     * Maximum number of events to process in single cleanup batch
     * @default 1000
     */
    cleanupBatchSize?: number;

    /**
     * Enable DLQ monitoring and metrics
     * @default true
     */
    enableMonitoring?: boolean;

    /**
     * Alert thresholds for DLQ monitoring
     */
    alertThresholds?: {
      /**
       * Alert when DLQ contains more than this number of events
       * @default 1000
       */
      maxEvents?: number;

      /**
       * Alert when DLQ growth rate exceeds this many events per hour
       * @default 100
       */
      maxGrowthRatePerHour?: number;

      /**
       * Alert when error rate for specific error type exceeds this percentage
       * @default 50
       */
      maxErrorRatePercent?: number;
    };
  };

  /**
   * Archive configuration
   */
  archive?: {
    /**
     * Enable automatic archival
     * @default true
     */
    enabled?: boolean;

    /**
     * Retention period in days for events before archival
     * @default 30
     */
    retentionDays?: number;

    /**
     * Batch size for archival operations
     * @default 1000
     */
    batchSize?: number;

    /**
     * Cron schedule for archival job
     * @default '0 2 * * *' (daily at 2 AM)
     */
    schedule?: string;

    /**
     * Enable compression for archived events
     * @default true
     */
    enableCompression?: boolean;

    /**
     * Archive table prefix
     * @default 'archived_'
     */
    tablePrefix?: string;

    /**
     * Maximum archive table size before cleanup
     * @default 1000000
     */
    maxArchiveTableSize?: number;

    /**
     * Enable archive table cleanup
     * @default true
     */
    enableCleanup?: boolean;

    /**
     * Cleanup interval in days
     * @default 90
     */
    cleanupIntervalDays?: number;
  };

  /**
   * Metrics configuration
   */
  metrics?: {
    /**
     * Enable metrics collection
     * @default true
     */
    enabled?: boolean;

    /**
     * Metrics collection interval in milliseconds
     * @default 15000 (15 seconds)
     */
    collectionIntervalMs?: number;

    /**
     * Enable detailed performance metrics
     * @default true
     */
    enablePerformanceMetrics?: boolean;

    /**
     * Enable business metrics
     * @default true
     */
    enableBusinessMetrics?: boolean;

    /**
     * Enable system metrics
     * @default true
     */
    enableSystemMetrics?: boolean;

    /**
     * Metrics prefix for all metrics
     * @default 'ms_event_stream'
     */
    metricsPrefix?: string;

    /**
     * Enable histogram buckets for latency metrics
     * @default true
     */
    enableHistograms?: boolean;

    /**
     * Custom histogram buckets for latency (in milliseconds)
     * @default [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
     */
    histogramBuckets?: number[];

    /**
     * Enable metrics caching
     * @default true
     */
    enableCaching?: boolean;

    /**
     * Cache TTL in milliseconds
     * @default 5000 (5 seconds)
     */
    cacheTtlMs?: number;
  };

  /**
   * Health check configuration
   */
  health?: {
    /**
     * Enable health checks
     * @default true
     */
    enabled?: boolean;

    /**
     * Health check interval in milliseconds
     * @default 30000 (30 seconds)
     */
    checkIntervalMs?: number;

    /**
     * Health check timeout in milliseconds
     * @default 5000 (5 seconds)
     */
    checkTimeoutMs?: number;

    /**
     * Enable detailed health checks
     * @default true
     */
    enableDetailedChecks?: boolean;

    /**
     * Enable performance health checks
     * @default true
     */
    enablePerformanceChecks?: boolean;

    /**
     * Enable business logic health checks
     * @default true
     */
    enableBusinessChecks?: boolean;

    /**
     * Health check failure threshold
     * @default 3
     */
    failureThreshold?: number;

    /**
     * Health check recovery threshold
     * @default 2
     */
    recoveryThreshold?: number;

    /**
     * Enable health check caching
     * @default true
     */
    enableCaching?: boolean;

    /**
     * Cache TTL in milliseconds
     * @default 10000 (10 seconds)
     */
    cacheTtlMs?: number;
  };

  /**
   * Schema registry configuration
   */
  schemaRegistry?: {
    /**
     * Enable schema registry
     * @default true
     */
    enabled?: boolean;

    /**
     * Enforce schema validation for all events
     * @default true
     */
    enforceValidation?: boolean;

    /**
     * Default compatibility level for new schemas
     * @default SchemaCompatibilityLevel.BACKWARD
     */
    defaultCompatibilityLevel?: SchemaCompatibilityLevel;

    /**
     * Schema evolution strategy
     * @default SchemaEvolutionStrategy.STRICT
     */
    evolutionStrategy?: SchemaEvolutionStrategy;

    /**
     * Allow schema deletion (not recommended for production)
     * @default false
     */
    allowSchemaDeletion?: boolean;

    /**
     * Maximum number of versions to keep per schema
     * @default 100
     */
    maxVersionsPerSchema?: number;

    /**
     * Enable schema caching
     * @default true
     */
    enableSchemaCache?: boolean;

    /**
     * Schema cache TTL in milliseconds
     * @default 60000 (1 minute)
     */
    schemaCacheTtlMs?: number;

    /**
     * Validate events on publish
     * @default true
     */
    validateOnPublish?: boolean;

    /**
     * Validate events on consume
     * @default true
     */
    validateOnConsume?: boolean;

    /**
     * Enable compatibility checking for schema updates
     * @default true
     */
    enableCompatibilityCheck?: boolean;

    /**
     * Enable schema deprecation
     * @default true
     */
    enableDeprecation?: boolean;

    /**
     * Enable schema versioning
     * @default true
     */
    enableVersioning?: boolean;

    /**
     * Enable schema tagging
     * @default true
     */
    enableTags?: boolean;
  };

  /**
   * Event versioning configuration
   */
  versioning?: {
    /**
     * Enable event versioning
     * @default true
     */
    enabled?: boolean;

    /**
     * Default version strategy for new events
     * @default EventVersionStrategy.BACKWARD
     */
    defaultVersionStrategy?: 'STRICT' | 'BACKWARD' | 'FORWARD' | 'FULL' | 'NONE';

    /**
     * Default migration strategy for new events
     * @default EventMigrationStrategy.AUTOMATIC
     */
    defaultMigrationStrategy?: 'AUTOMATIC' | 'MANUAL' | 'NONE';

    /**
     * Enable version caching
     * @default true
     */
    enableVersionCache?: boolean;

    /**
     * Version cache TTL in milliseconds
     * @default 60000 (1 minute)
     */
    versionCacheTtlMs?: number;

    /**
     * Enable version validation
     * @default true
     */
    enableValidation?: boolean;

    /**
     * Enable version compatibility checking
     * @default true
     */
    enableCompatibilityCheck?: boolean;

    /**
     * Enable version deprecation
     * @default true
     */
    enableDeprecation?: boolean;

    /**
     * Enable version tagging
     * @default true
     */
    enableTags?: boolean;

    /**
     * Maximum number of versions to keep per event type
     * @default 100
     */
    maxVersionsPerEventType?: number;

    /**
     * Allow version deletion (not recommended for production)
     * @default false
     */
    allowVersionDeletion?: boolean;

    /**
     * Enable version history
     * @default true
     */
    enableHistory?: boolean;

    /**
     * Maximum number of history entries to keep
     * @default 1000
     */
    maxHistorySize?: number;

    /**
     * Enable version monitoring
     * @default true
     */
    enableMonitoring?: boolean;

    /**
     * Enable version notifications
     * @default true
     */
    enableNotifications?: boolean;

    /**
     * Enable automatic version cleanup
     * @default true
     */
    enableCleanup?: boolean;

    /**
     * Version retention period in days
     * @default 90
     */
    retentionDays?: number;

    /**
     * Cleanup interval in milliseconds
     * @default 86400000 (24 hours)
     */
    cleanupIntervalMs?: number;
  };

  /**
   * Event replay configuration
   */
  replay?: {
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
  };
}

/**
 * Internal configuration with defaults applied
 */
export interface ResolvedEventStreamConfig extends Required<EventStreamConfig> {
  database: Required<NonNullable<EventStreamConfig['database']>>;
  redis: Required<NonNullable<EventStreamConfig['redis']>>;
  events: Required<NonNullable<EventStreamConfig['events']>>;
  outbox: Required<NonNullable<EventStreamConfig['outbox']>> & {
    retry: Required<NonNullable<EventStreamConfig['outbox']>['retry']>;
  };
  inbox: Required<NonNullable<EventStreamConfig['inbox']>> & {
    retry: Required<NonNullable<EventStreamConfig['inbox']>['retry']>;
  };
  consumer: Required<NonNullable<EventStreamConfig['consumer']>>;
  discovery: Required<NonNullable<EventStreamConfig['discovery']>>;
  circuitBreaker: Required<NonNullable<EventStreamConfig['circuitBreaker']>>;
  retry: Required<NonNullable<EventStreamConfig['retry']>>;
  dlq: Required<NonNullable<EventStreamConfig['dlq']>> & {
    alertThresholds: Required<NonNullable<EventStreamConfig['dlq']>['alertThresholds']>;
  };
  archive: Required<NonNullable<EventStreamConfig['archive']>>;
  metrics: Required<NonNullable<EventStreamConfig['metrics']>>;
  health: Required<NonNullable<EventStreamConfig['health']>>;
  schemaRegistry: Required<NonNullable<EventStreamConfig['schemaRegistry']>>;
  versioning: Required<NonNullable<EventStreamConfig['versioning']>>;
  replay: Required<NonNullable<EventStreamConfig['replay']>>;
}
