import { DynamicModule, Global, Module, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ModuleRef, DiscoveryModule } from '@nestjs/core';
import { EventStreamService } from './event-stream.service';
import { DatabaseAdapter } from './adapters/db.adapter';
import { RedisAdapter } from './adapters/redis.adapter';
import { EventHandlerDiscovery } from './discovery/event-handler.discovery';
import { RetryManager } from './managers/retry.manager';
import { DLQManager } from './managers/dlq.manager';
import { ArchiveManager } from './managers/archive.manager';
import { ArchiverJob } from './jobs/archiver.job';
import { MetricsCollector } from './observability/metrics.collector';
import { HealthChecker } from './observability/health.checker';
import { SchemaRegistry } from './schema/schema.registry';
import { ReplayManager } from './managers/replay.manager';
import { PointInTimeRecovery } from './recovery/point-in-time.recovery';
import { EventVersionManager } from './versioning/version.manager';
import {
  EventStreamConfig,
  ResolvedEventStreamConfig,
  Environment,
  SchemaCompatibilityLevel,
  SchemaEvolutionStrategy,
} from './interfaces';

/**
 * Token for the EventStream configuration
 */
export const EVENT_STREAM_CONFIG = Symbol('EVENT_STREAM_CONFIG');

/**
 * Default configuration values
 */
const DEFAULT_CONFIG: Partial<EventStreamConfig> = {
  env: 'development' as Environment,
  database: {
    maxConnections: 10,
    connectionTimeoutMs: 5000,
    idleTimeoutMs: 30000,
  },
  redis: {
    retryAttempts: 3,
    retryDelayMs: 1000,
    connectionTimeoutMs: 5000,
    maxStreamLength: 100000,
  },
  events: {
    maxConcurrentEvents: 10,
    processingTimeoutMs: 30000,
    enableDeduplication: true,
  },
  outbox: {
    maxRetries: 3,
    retentionDays: 7,
    enableCleanup: true,
    cleanupIntervalMs: 86400000, // 24 hours
    retry: {
      baseDelayMs: 1000,
      maxDelayMs: 30000,
      backoffMultiplier: 2,
      enableJitter: true,
      jitterPercent: 0.1,
    },
  },
  inbox: {
    maxRetries: 3,
    retentionDays: 7,
    enableCleanup: true,
    cleanupIntervalMs: 86400000, // 24 hours
    retry: {
      baseDelayMs: 1000,
      maxDelayMs: 30000,
      backoffMultiplier: 2,
      enableJitter: true,
      jitterPercent: 0.1,
    },
  },
  consumer: {
    batchSize: 10,
    blockTimeMs: 1000,
    pollIntervalMs: 5000,
    maxRetries: 3,
    shutdownTimeoutMs: 30000,
    enableHealthChecks: true,
    maxConcurrentConsumers: 1,
    autoStart: true,
  },
  discovery: {
    enabled: true,
    includeModules: [],
    excludeModules: [],
    validateHandlers: true,
    throwOnError: false,
    enableLogging: true,
  },
  circuitBreaker: {
    enabled: true,
    failureThreshold: 5,
    recoveryTimeoutMs: 60000,
    timeWindowMs: 60000,
    minimumRequestThreshold: 10,
  },
  retry: {
    enabled: true,
    defaultMaxAttempts: 3,
    defaultBaseDelayMs: 1000,
    defaultMaxDelayMs: 30000,
    defaultBackoffMultiplier: 2,
    defaultEnableJitter: true,
    defaultJitterPercent: 0.1,
  },
  dlq: {
    enabled: true,
    maxStreamLength: 10000,
    retentionDays: 30,
    enableCleanup: true,
    cleanupIntervalMs: 86400000, // 24 hours
    cleanupBatchSize: 1000,
    enableMonitoring: true,
    alertThresholds: {
      maxEvents: 1000,
      maxGrowthRatePerHour: 100,
      maxErrorRatePercent: 50,
    },
  },
  archive: {
    enabled: true,
    retentionDays: 30,
    batchSize: 1000,
    schedule: '0 2 * * *', // Daily at 2 AM
    enableCompression: true,
    tablePrefix: 'archived_',
    maxArchiveTableSize: 1000000,
    enableCleanup: true,
    cleanupIntervalDays: 90,
  },
  metrics: {
    enabled: true,
    collectionIntervalMs: 15000,
    enablePerformanceMetrics: true,
    enableBusinessMetrics: true,
    enableSystemMetrics: true,
          metricsPrefix: 'ms_event_stream',
    enableHistograms: true,
    histogramBuckets: [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000],
    enableCaching: true,
    cacheTtlMs: 5000,
  },
  health: {
    enabled: true,
    checkIntervalMs: 30000,
    checkTimeoutMs: 5000,
    enableDetailedChecks: true,
    enablePerformanceChecks: true,
    enableBusinessChecks: true,
    failureThreshold: 3,
    recoveryThreshold: 2,
    enableCaching: true,
    cacheTtlMs: 10000,
  },
  schemaRegistry: {
    enabled: true,
    enforceValidation: true,
    defaultCompatibilityLevel: SchemaCompatibilityLevel.BACKWARD,
    evolutionStrategy: SchemaEvolutionStrategy.STRICT,
    allowSchemaDeletion: false,
    maxVersionsPerSchema: 100,
    enableSchemaCache: true,
    schemaCacheTtlMs: 60000,
    validateOnPublish: true,
    validateOnConsume: true,
    enableCompatibilityCheck: true,
    enableDeprecation: true,
    enableVersioning: true,
    enableTags: true,
  },
  versioning: {
    enabled: true,
    defaultVersionStrategy: 'BACKWARD',
    defaultMigrationStrategy: 'AUTOMATIC',
    enableVersionCache: true,
    versionCacheTtlMs: 60000,
    enableValidation: true,
    enableCompatibilityCheck: true,
    enableDeprecation: true,
    enableTags: true,
    maxVersionsPerEventType: 100,
    allowVersionDeletion: false,
    enableHistory: true,
    maxHistorySize: 1000,
    enableMonitoring: true,
    enableNotifications: true,
    enableCleanup: true,
    retentionDays: 90,
    cleanupIntervalMs: 86400000,
  },
};

/**
 * EventStream module that provides event publishing and consuming capabilities
 * This is the main module that applications will import
 */
@Global()
@Module({})
export class EventStreamModule implements OnModuleInit, OnApplicationShutdown {
  constructor(private readonly moduleRef: ModuleRef) {}

  /**
   * Configure the EventStream module with the provided configuration
   * @param config - EventStream configuration
   */
  static forRoot(config: EventStreamConfig): DynamicModule {
    // Validate required configuration
    EventStreamModule.validateConfig(config);

    // Merge with defaults to create resolved config
    const resolvedConfig: ResolvedEventStreamConfig = {
      ...config,
      env: config.env || DEFAULT_CONFIG.env!,
      database: {
        ...DEFAULT_CONFIG.database!,
        ...config.database,
      } as Required<NonNullable<EventStreamConfig['database']>>,
      redis: {
        ...DEFAULT_CONFIG.redis!,
        ...config.redis,
      } as Required<NonNullable<EventStreamConfig['redis']>>,
      events: {
        ...DEFAULT_CONFIG.events!,
        ...config.events,
      } as Required<NonNullable<EventStreamConfig['events']>>,
      outbox: {
        ...DEFAULT_CONFIG.outbox!,
        ...config.outbox,
        retry: {
          ...DEFAULT_CONFIG.outbox!.retry!,
          ...config.outbox?.retry,
        },
      } as Required<NonNullable<EventStreamConfig['outbox']>> & {
        retry: Required<NonNullable<EventStreamConfig['outbox']>['retry']>;
      },
      inbox: {
        ...DEFAULT_CONFIG.inbox!,
        ...config.inbox,
        retry: {
          ...DEFAULT_CONFIG.inbox!.retry!,
          ...config.inbox?.retry,
        },
      } as Required<NonNullable<EventStreamConfig['inbox']>> & {
        retry: Required<NonNullable<EventStreamConfig['inbox']>['retry']>;
      },
      consumer: {
        ...DEFAULT_CONFIG.consumer!,
        ...config.consumer,
      } as Required<NonNullable<EventStreamConfig['consumer']>>,
      discovery: {
        ...DEFAULT_CONFIG.discovery!,
        ...config.discovery,
      } as Required<NonNullable<EventStreamConfig['discovery']>>,
      circuitBreaker: {
        ...DEFAULT_CONFIG.circuitBreaker!,
        ...config.circuitBreaker,
      } as Required<NonNullable<EventStreamConfig['circuitBreaker']>>,
      retry: {
        ...DEFAULT_CONFIG.retry!,
        ...config.retry,
      } as Required<NonNullable<EventStreamConfig['retry']>>,
      dlq: {
        ...DEFAULT_CONFIG.dlq!,
        ...config.dlq,
        alertThresholds: {
          ...DEFAULT_CONFIG.dlq!.alertThresholds!,
          ...config.dlq?.alertThresholds,
        },
      } as Required<NonNullable<EventStreamConfig['dlq']>> & {
        alertThresholds: Required<NonNullable<EventStreamConfig['dlq']>['alertThresholds']>;
      },
      archive: {
        ...DEFAULT_CONFIG.archive!,
        ...config.archive,
      } as Required<NonNullable<EventStreamConfig['archive']>>,
      metrics: {
        ...DEFAULT_CONFIG.metrics!,
        ...config.metrics,
      } as Required<NonNullable<EventStreamConfig['metrics']>>,
      health: {
        ...DEFAULT_CONFIG.health!,
        ...config.health,
      } as Required<NonNullable<EventStreamConfig['health']>>,
      schemaRegistry: {
        ...DEFAULT_CONFIG.schemaRegistry!,
        ...config.schemaRegistry,
      } as Required<NonNullable<EventStreamConfig['schemaRegistry']>>,
      versioning: {
        enabled: true,
        defaultVersionStrategy: 'BACKWARD',
        defaultMigrationStrategy: 'AUTOMATIC',
        enableVersionCache: true,
        versionCacheTtlMs: 60000,
        enableValidation: true,
        enableCompatibilityCheck: true,
        enableDeprecation: true,
        enableTags: true,
        maxVersionsPerEventType: 100,
        allowVersionDeletion: false,
        enableHistory: true,
        maxHistorySize: 1000,
        enableMonitoring: true,
        enableNotifications: true,
        enableCleanup: true,
        retentionDays: 90,
        cleanupIntervalMs: 86400000,
        ...config.versioning,
      } as Required<NonNullable<EventStreamConfig['versioning']>>,
      replay: {
        enabled: true,
        maxConcurrentEvents: 10,
        maxRetries: 3,
        batchSize: 100,
        enableProgressTracking: true,
        enableMonitoring: true,
        enableHistory: true,
        maxHistorySize: 100,
        enableValidation: true,
        enableNotifications: true,
        ...config.replay,
      } as Required<NonNullable<EventStreamConfig['replay']>>,
    };

    return {
      module: EventStreamModule,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: EVENT_STREAM_CONFIG,
          useValue: resolvedConfig,
        },
        {
          provide: DatabaseAdapter,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new DatabaseAdapter(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: RedisAdapter,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new RedisAdapter(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: RetryManager,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new RetryManager(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: DLQManager,
          useFactory: (config: ResolvedEventStreamConfig, redisAdapter: RedisAdapter, retryManager: RetryManager) => {
            return new DLQManager(config, redisAdapter, retryManager);
          },
          inject: [EVENT_STREAM_CONFIG, RedisAdapter, RetryManager],
        },
        {
          provide: ArchiveManager,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new ArchiveManager(dbAdapter, redisAdapter, config);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: ArchiverJob,
          useFactory: (archiveManager: ArchiveManager) => {
            return new ArchiverJob(archiveManager);
          },
          inject: [ArchiveManager],
        },
        {
          provide: MetricsCollector,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new MetricsCollector(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: HealthChecker,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new HealthChecker(dbAdapter, redisAdapter, config);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: SchemaRegistry,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter) => {
            return new SchemaRegistry(config, dbAdapter);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter],
        },
        {
          provide: ReplayManager,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new ReplayManager(config, dbAdapter, redisAdapter);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: PointInTimeRecovery,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter, replayManager: ReplayManager) => {
            return new PointInTimeRecovery(config, dbAdapter, redisAdapter, replayManager);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter, ReplayManager],
        },
        {
          provide: EventVersionManager,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new EventVersionManager(config, dbAdapter, redisAdapter);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: EventStreamService,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter, retryManager: RetryManager, dlqManager: DLQManager, archiveManager: ArchiveManager, metricsCollector: MetricsCollector, healthChecker: HealthChecker, schemaRegistry: SchemaRegistry, replayManager: ReplayManager, pointInTimeRecovery: PointInTimeRecovery, versionManager: EventVersionManager) => {
            return new EventStreamService(config, dbAdapter, redisAdapter, retryManager, dlqManager, archiveManager, metricsCollector, healthChecker, schemaRegistry, replayManager, pointInTimeRecovery, versionManager);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter, RetryManager, DLQManager, ArchiveManager, MetricsCollector, HealthChecker, SchemaRegistry, ReplayManager, PointInTimeRecovery, EventVersionManager],
        },
        EventHandlerDiscovery,
      ],
      exports: [EventStreamService, DatabaseAdapter, RedisAdapter, RetryManager, DLQManager, ArchiveManager, ArchiverJob, MetricsCollector, HealthChecker, SchemaRegistry, ReplayManager, PointInTimeRecovery, EventVersionManager, EventHandlerDiscovery, EVENT_STREAM_CONFIG],
    };
  }

  /**
   * Configure the EventStream module asynchronously
   * Useful when configuration needs to be loaded from external sources
   */
  static forRootAsync(options: {
    useFactory: (...args: any[]) => Promise<EventStreamConfig> | EventStreamConfig;
    inject?: any[];
  }): DynamicModule {
    return {
      module: EventStreamModule,
      imports: [DiscoveryModule],
      providers: [
        {
          provide: EVENT_STREAM_CONFIG,
          useFactory: async (...args: any[]) => {
            const config = await options.useFactory(...args);
            EventStreamModule.validateConfig(config);

            // Merge with defaults
            const resolvedConfig: ResolvedEventStreamConfig = {
              ...config,
              env: config.env || DEFAULT_CONFIG.env!,
              database: {
                ...DEFAULT_CONFIG.database!,
                ...config.database,
              } as Required<NonNullable<EventStreamConfig['database']>>,
              redis: {
                ...DEFAULT_CONFIG.redis!,
                ...config.redis,
              } as Required<NonNullable<EventStreamConfig['redis']>>,
              events: {
                ...DEFAULT_CONFIG.events!,
                ...config.events,
              } as Required<NonNullable<EventStreamConfig['events']>>,
              outbox: {
                ...DEFAULT_CONFIG.outbox!,
                ...config.outbox,
                retry: {
                  ...DEFAULT_CONFIG.outbox!.retry!,
                  ...config.outbox?.retry,
                },
              } as Required<NonNullable<EventStreamConfig['outbox']>> & {
                retry: Required<NonNullable<EventStreamConfig['outbox']>['retry']>;
              },
              inbox: {
                ...DEFAULT_CONFIG.inbox!,
                ...config.inbox,
                retry: {
                  ...DEFAULT_CONFIG.inbox!.retry!,
                  ...config.inbox?.retry,
                },
              } as Required<NonNullable<EventStreamConfig['inbox']>> & {
                retry: Required<NonNullable<EventStreamConfig['inbox']>['retry']>;
              },
              consumer: {
                ...DEFAULT_CONFIG.consumer!,
                ...config.consumer,
              } as Required<NonNullable<EventStreamConfig['consumer']>>,
              discovery: {
                ...DEFAULT_CONFIG.discovery!,
                ...config.discovery,
              } as Required<NonNullable<EventStreamConfig['discovery']>>,
              circuitBreaker: {
                ...DEFAULT_CONFIG.circuitBreaker!,
                ...config.circuitBreaker,
              } as Required<NonNullable<EventStreamConfig['circuitBreaker']>>,
              retry: {
                ...DEFAULT_CONFIG.retry!,
                ...config.retry,
              } as Required<NonNullable<EventStreamConfig['retry']>>,
              dlq: {
                ...DEFAULT_CONFIG.dlq!,
                ...config.dlq,
                alertThresholds: {
                  ...DEFAULT_CONFIG.dlq!.alertThresholds!,
                  ...config.dlq?.alertThresholds,
                },
              } as Required<NonNullable<EventStreamConfig['dlq']>> & {
                alertThresholds: Required<NonNullable<EventStreamConfig['dlq']>['alertThresholds']>;
              },
              archive: {
                ...DEFAULT_CONFIG.archive!,
                ...config.archive,
              } as Required<NonNullable<EventStreamConfig['archive']>>,
              metrics: {
                ...DEFAULT_CONFIG.metrics!,
                ...config.metrics,
              } as Required<NonNullable<EventStreamConfig['metrics']>>,
              health: {
                ...DEFAULT_CONFIG.health!,
                ...config.health,
              } as Required<NonNullable<EventStreamConfig['health']>>,
              schemaRegistry: {
                ...DEFAULT_CONFIG.schemaRegistry!,
                ...config.schemaRegistry,
              } as Required<NonNullable<EventStreamConfig['schemaRegistry']>>,
              versioning: {
                enabled: true,
                defaultVersionStrategy: 'BACKWARD',
                defaultMigrationStrategy: 'AUTOMATIC',
                enableVersionCache: true,
                versionCacheTtlMs: 60000,
                enableValidation: true,
                enableCompatibilityCheck: true,
                enableDeprecation: true,
                enableTags: true,
                maxVersionsPerEventType: 100,
                allowVersionDeletion: false,
                enableHistory: true,
                maxHistorySize: 1000,
                enableMonitoring: true,
                enableNotifications: true,
                enableCleanup: true,
                retentionDays: 90,
                cleanupIntervalMs: 86400000,
                ...config.versioning,
              } as Required<NonNullable<EventStreamConfig['versioning']>>,
              replay: {
                enabled: true,
                maxConcurrentEvents: 10,
                maxRetries: 3,
                batchSize: 100,
                enableProgressTracking: true,
                enableMonitoring: true,
                enableHistory: true,
                maxHistorySize: 100,
                enableValidation: true,
                enableNotifications: true,
                ...config.replay,
              } as Required<NonNullable<EventStreamConfig['replay']>>,
            };

            return resolvedConfig;
          },
          inject: options.inject || [],
        },
        {
          provide: DatabaseAdapter,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new DatabaseAdapter(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: RedisAdapter,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new RedisAdapter(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: RetryManager,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new RetryManager(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: DLQManager,
          useFactory: (config: ResolvedEventStreamConfig, redisAdapter: RedisAdapter, retryManager: RetryManager) => {
            return new DLQManager(config, redisAdapter, retryManager);
          },
          inject: [EVENT_STREAM_CONFIG, RedisAdapter, RetryManager],
        },
        {
          provide: ArchiveManager,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new ArchiveManager(dbAdapter, redisAdapter, config);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: ArchiverJob,
          useFactory: (archiveManager: ArchiveManager) => {
            return new ArchiverJob(archiveManager);
          },
          inject: [ArchiveManager],
        },
        {
          provide: MetricsCollector,
          useFactory: (config: ResolvedEventStreamConfig) => {
            return new MetricsCollector(config);
          },
          inject: [EVENT_STREAM_CONFIG],
        },
        {
          provide: HealthChecker,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new HealthChecker(dbAdapter, redisAdapter, config);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: SchemaRegistry,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter) => {
            return new SchemaRegistry(config, dbAdapter);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter],
        },
        {
          provide: ReplayManager,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new ReplayManager(config, dbAdapter, redisAdapter);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: PointInTimeRecovery,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter, replayManager: ReplayManager) => {
            return new PointInTimeRecovery(config, dbAdapter, redisAdapter, replayManager);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter, ReplayManager],
        },
        {
          provide: EventVersionManager,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter) => {
            return new EventVersionManager(config, dbAdapter, redisAdapter);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter],
        },
        {
          provide: EventStreamService,
          useFactory: (config: ResolvedEventStreamConfig, dbAdapter: DatabaseAdapter, redisAdapter: RedisAdapter, retryManager: RetryManager, dlqManager: DLQManager, archiveManager: ArchiveManager, metricsCollector: MetricsCollector, healthChecker: HealthChecker, schemaRegistry: SchemaRegistry, replayManager: ReplayManager, pointInTimeRecovery: PointInTimeRecovery, versionManager: EventVersionManager) => {
            return new EventStreamService(config, dbAdapter, redisAdapter, retryManager, dlqManager, archiveManager, metricsCollector, healthChecker, schemaRegistry, replayManager, pointInTimeRecovery, versionManager);
          },
          inject: [EVENT_STREAM_CONFIG, DatabaseAdapter, RedisAdapter, RetryManager, DLQManager, ArchiveManager, MetricsCollector, HealthChecker, SchemaRegistry, ReplayManager, PointInTimeRecovery, EventVersionManager],
        },
        EventHandlerDiscovery,
      ],
      exports: [EventStreamService, DatabaseAdapter, RedisAdapter, RetryManager, DLQManager, ArchiveManager, ArchiverJob, MetricsCollector, HealthChecker, SchemaRegistry, ReplayManager, PointInTimeRecovery, EventVersionManager, EventHandlerDiscovery, EVENT_STREAM_CONFIG],
    };
  }

  /**
   * Initialize the module and start the event stream service
   */
  async onModuleInit(): Promise<void> {
    const eventStreamService = this.moduleRef.get(EventStreamService, { strict: false });
    await eventStreamService.initialize();
  }

  /**
   * Gracefully shutdown the service
   */
  async onApplicationShutdown(): Promise<void> {
    const eventStreamService = this.moduleRef.get(EventStreamService, { strict: false });
    await eventStreamService.shutdown();
  }

  /**
   * Validate the provided configuration
   * @param config - Configuration to validate
   */
  private static validateConfig(config: EventStreamConfig): void {
    if (!config.serviceName) {
      throw new Error('EventStreamConfig: serviceName is required');
    }

    if (!config.dbUrl) {
      throw new Error('EventStreamConfig: dbUrl is required');
    }

    if (!config.redisUrl) {
      throw new Error('EventStreamConfig: redisUrl is required');
    }

    // Validate service name format (should be a valid identifier)
    if (!/^[a-zA-Z][a-zA-Z0-9-_]*$/.test(config.serviceName)) {
      throw new Error(
        'EventStreamConfig: serviceName must start with a letter and contain only letters, numbers, hyphens, and underscores',
      );
    }

    // Validate URLs format (basic validation)
    try {
      new URL(config.dbUrl);
    } catch {
      throw new Error('EventStreamConfig: dbUrl must be a valid URL');
    }

    try {
      new URL(config.redisUrl);
    } catch {
      throw new Error('EventStreamConfig: redisUrl must be a valid URL');
    }

    // Validate environment
    if (config.env && !['development', 'staging', 'production'].includes(config.env)) {
      throw new Error('EventStreamConfig: env must be one of: development, staging, production');
    }

    // Validate numeric values
    if (config.database?.maxConnections && config.database.maxConnections <= 0) {
      throw new Error('EventStreamConfig: database.maxConnections must be greater than 0');
    }

    if (config.events?.maxConcurrentEvents && config.events.maxConcurrentEvents <= 0) {
      throw new Error('EventStreamConfig: events.maxConcurrentEvents must be greater than 0');
    }
  }
}