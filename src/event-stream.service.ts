import { Injectable, Logger } from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';
import { DatabaseAdapter } from './adapters/db.adapter';
import { RedisAdapter } from './adapters/redis.adapter';
import { OutboxManager } from './managers/outbox.manager';
import { InboxManager } from './managers/inbox.manager';
import { RetryManager } from './managers/retry.manager';
import { DLQManager } from './managers/dlq.manager';
import { ArchiveManager } from './managers/archive.manager';
import { MetricsCollector } from './observability/metrics.collector';
import { HealthChecker } from './observability/health.checker';
import { SchemaRegistry } from './schema/schema.registry';
import { ReplayManager } from './managers/replay.manager';
import { PointInTimeRecovery, PointInTimeRecoveryOptions, PointInTimeRecoveryResult } from './recovery/point-in-time.recovery';
import { EventVersionManager } from './versioning/version.manager';
import { EventVersionStrategy, EventMigrationStrategy, EventVersion, EventVersionRegistrationOptions, EventVersionUpdateOptions, EventVersionCompatibilityResult, EventVersionMigration } from './versioning/interfaces';
import { EventMigrationStrategyFactory } from './versioning/migration.strategies';
import { JSONSchema7 } from 'json-schema';
import { EventConsumer, ConsumerHealth, ConsumerStats } from './consumers/event.consumer';
import { HandlerRegistry, HandlerStats } from './registries/handler.registry';
import {
  EventPayload,
  PublishOptions,
  EventHandlerMetadata,
  ResolvedEventStreamConfig,
  SchemaMetadata,
  SchemaVersion,
  SchemaValidationResult,
  SchemaRegistrationOptions,
  SchemaUpdateOptions,
  SchemaMigrationPlan,
  ReplayOptions,
  ReplayHistory,
  ReplayProgress,
} from './interfaces';

/**
 * Core service for event publishing and management
 * This is the main service that applications will interact with
 */
@Injectable()
export class EventStreamService {
  private readonly logger = new Logger(EventStreamService.name);
  private isInitialized = false;
  private outboxManager: OutboxManager;
  private inboxManager: InboxManager;
  private handlerRegistry: HandlerRegistry;
  private eventConsumer: EventConsumer;
  private isConsumerRunning = false;

  constructor(
    private readonly config: ResolvedEventStreamConfig,
    private readonly databaseAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
    private readonly retryManager: RetryManager,
    private readonly dlqManager: DLQManager,
    private readonly archiveManager: ArchiveManager,
    private readonly metricsCollector: MetricsCollector,
    private readonly healthChecker: HealthChecker,
    private readonly schemaRegistry: SchemaRegistry,
    private readonly replayManager: ReplayManager,
    private readonly pointInTimeRecovery: PointInTimeRecovery,
    private readonly versionManager: EventVersionManager,
  ) {
    // Initialize managers and services
    this.outboxManager = new OutboxManager(config, databaseAdapter, redisAdapter, retryManager, dlqManager);
    this.inboxManager = new InboxManager(config, databaseAdapter, retryManager, dlqManager);
    this.handlerRegistry = new HandlerRegistry();
    this.eventConsumer = new EventConsumer(config, redisAdapter, this.inboxManager);
  }

  /**
   * Initialize the event stream service
   * This should be called during module initialization
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) {
      this.logger.warn('EventStreamService is already initialized');
      return;
    }

    this.logger.log(`Initializing EventStreamService for service: ${this.config.serviceName}`);
    
    try {
      // Phase 2 - Initialize database connection and create tables
      this.logger.log('Initializing database adapter...');
      await this.databaseAdapter.initialize();
      this.logger.log('Database adapter initialized successfully');
      
      // Phase 3 - Initialize Redis connection and setup streams
      this.logger.log('Initializing Redis adapter...');
      await this.redisAdapter.initialize();
      this.logger.log('Redis adapter initialized successfully');
      
      // Phase 10 - Initialize archive manager
      this.logger.log('Initializing archive manager...');
      await this.archiveManager.initialize();
      this.logger.log('Archive manager initialized successfully');
      
      // Start scheduled archival jobs
      this.archiveManager.startScheduledArchival();
      this.logger.log('Archive scheduled jobs started');
      
      // Phase 11 - Start observability services
      this.logger.log('Starting observability services...');
      if (this.config.metrics.enabled) {
        this.metricsCollector.startCollection();
        this.logger.log('Metrics collection started');
      }
      if (this.config.health.enabled) {
        this.healthChecker.startHealthChecks();
        this.logger.log('Health checks started');
      }

      // Phase 12 - Initialize schema registry
      this.logger.log('Initializing schema registry...');
      if (this.config.schemaRegistry.enabled) {
        await this.schemaRegistry.initialize();
        this.logger.log('Schema registry initialized successfully');
      }

      // Phase 13 - Initialize replay functionality
      this.logger.log('Initializing replay functionality...');
      if (this.config.replay.enabled) {
        await this.replayManager.initialize();
        await this.pointInTimeRecovery.initialize();
        this.logger.log('Replay functionality initialized successfully');
      }

      // Phase 14 - Initialize version manager
      this.logger.log('Initializing version manager...');
      if (this.config.versioning.enabled) {
        await this.versionManager.initialize();
        this.logger.log('Version manager initialized successfully');
      }
      
      // Phase 6 - Start event consumers if auto-start is enabled
      if (this.config.consumer.autoStart) {
        this.logger.log('Starting event consumer...');
        await this.startConsumer();
        this.logger.log('Event consumer started successfully');
      } else {
        this.logger.log('Auto-start disabled, consumer not started automatically');
      }
      
      this.isInitialized = true;
      this.logger.log('EventStreamService initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize EventStreamService', error);
      throw error;
    }
  }

  /**
   * Publish an event using the outbox pattern
   * @param eventType - The type of event (e.g., "order.created")
   * @param payload - The event data
   * @param options - Optional publishing options
   */
  async publish<T = any>(
    eventType: string,
    payload: T,
    options?: PublishOptions,
  ): Promise<void> {
    this.ensureInitialized();

    const eventId = options?.eventId || uuidv4();
    const correlationId = options?.correlationId || uuidv4();
    const traceId = options?.traceId;
    const version = options?.version || '1.0';

    this.logger.debug(`Publishing event: ${eventType} with ID: ${eventId}`);

    try {
      // Apply delay if specified
      if (options?.delayMs && options.delayMs > 0) {
        this.logger.debug(`Delaying event publication by ${options.delayMs}ms: ${eventType} (${eventId})`);
        await new Promise(resolve => setTimeout(resolve, options.delayMs));
      }

      // Validate event schema if enabled
      if (this.config.schemaRegistry.enabled && this.config.schemaRegistry.validateOnPublish) {
        const [namespace, name] = eventType.split('.');
        if (!namespace || !name) {
          throw new Error('Invalid event type format. Expected format: "namespace.name"');
        }

        const validationResult = await this.schemaRegistry.validateData(namespace, name, payload);
        if (!validationResult.isValid) {
          throw new Error(`Event validation failed: ${validationResult.errors?.join('; ')}`);
        }

        if (validationResult.warnings?.length) {
          this.logger.warn(`Event validation warnings: ${validationResult.warnings.join('; ')}`);
        }
      }

      // Handle event versioning if enabled
      if (this.config.versioning.enabled) {
        // Get event version
        const eventVersion = await this.versionManager.findVersion(eventType, version);
        if (!eventVersion) {
          throw new Error(`Event version not found: ${eventType} v${version}`);
        }

        // Validate event data against version schema
        const validationResult = await this.versionManager.validateData(eventType, version, payload);
        if (!validationResult.isValid) {
          throw new Error(`Event validation failed: ${validationResult.errors?.join('; ')}`);
        }

        if (validationResult.warnings?.length) {
          this.logger.warn(`Event validation warnings: ${validationResult.warnings.join('; ')}`);
        }

        // Check if version is active
        if (eventVersion.status !== 'ACTIVE') {
          throw new Error(`Event version ${version} is not active (status: ${eventVersion.status})`);
        }
      }

      // Use outbox pattern for reliable event publishing
      const result = await this.outboxManager.publishEventWithValidation({
        eventId,
        eventType,
        payload: payload as Record<string, any>,
        correlationId,
        traceId,
        version
      });

      if (!result.success) {
        const error = new Error(`Failed to publish event: ${result.error}`);
        // Add retry information to error for better error handling
        (error as any).retryable = result.retryable;
        (error as any).eventId = result.eventId;
        throw error;
      }
      
      this.logger.log(`Event published successfully: ${eventType} (${eventId})`);
    } catch (error) {
      this.logger.error(`Failed to publish event: ${eventType} (${eventId})`, error);
      throw error;
    }
  }

  /**
   * Register an event handler
   * This is typically called by the @OnEvent decorator
   * @param metadata - Handler metadata including event type and handler function
   */
  registerEventHandler(metadata: EventHandlerMetadata): void {
    this.handlerRegistry.registerHandler(metadata);
    this.logger.debug(`Registered event handler for: ${metadata.eventType}`);
  }

  /**
   * Get all registered handlers for an event type
   * @param eventType - The event type to get handlers for
   */
  getEventHandlers(eventType: string): EventHandlerMetadata[] {
    return this.handlerRegistry.getHandlers(eventType);
  }

  /**
   * Get all registered event types
   */
  getRegisteredEventTypes(): string[] {
    return this.handlerRegistry.getRegisteredEventTypes();
  }

  /**
   * Process an event by calling all registered handlers
   * This is used internally by the consumer system
   * @param eventPayload - The event payload to process
   */
  async processEvent(eventPayload: EventPayload): Promise<void> {
    this.ensureInitialized();

    const { eventType, eventId } = eventPayload.metadata;

    this.logger.debug(`Processing event: ${eventType} (${eventId})`);

    try {
      // Validate event schema if enabled
      if (this.config.schemaRegistry.enabled && this.config.schemaRegistry.validateOnConsume) {
        const [namespace, name] = eventType.split('.');
        if (!namespace || !name) {
          throw new Error('Invalid event type format. Expected format: "namespace.name"');
        }

        const validationResult = await this.schemaRegistry.validateData(namespace, name, eventPayload.data);
        if (!validationResult.isValid) {
          throw new Error(`Event validation failed: ${validationResult.errors?.join('; ')}`);
        }

        if (validationResult.warnings?.length) {
          this.logger.warn(`Event validation warnings: ${validationResult.warnings.join('; ')}`);
        }
      }

      // Handle event versioning if enabled
      if (this.config.versioning.enabled) {
        const version = eventPayload.metadata.version;

        // Get event version
        const eventVersion = await this.versionManager.findVersion(eventType, version || '1.0.0');
        if (!eventVersion) {
          throw new Error(`Event version not found: ${eventType} v${version || '1.0.0'}`);
        }

        // Validate event data against version schema
        const validationResult = await this.versionManager.validateData(eventType, version || '1.0.0', eventPayload.data);
        if (!validationResult.isValid) {
          throw new Error(`Event validation failed: ${validationResult.errors?.join('; ')}`);
        }

        if (validationResult.warnings?.length) {
          this.logger.warn(`Event validation warnings: ${validationResult.warnings.join('; ')}`);
        }

        // Check if version is active or deprecated (but not end of life)
        if (eventVersion.status === 'END_OF_LIFE') {
          throw new Error(`Event version ${version || '1.0.0'} has reached end of life`);
        }

        // Get latest version
        const latestVersion = await this.versionManager.getLatestVersion(eventType);
        if (latestVersion && latestVersion.version !== (version || '1.0.0')) {
          // Check compatibility
          const compatibilityResult = await this.versionManager.checkCompatibility(
            eventType,
            version || '1.0.0',
            latestVersion.version
          );

          if (!compatibilityResult.isCompatible) {
            throw new Error(`Event version ${version} is not compatible with latest version ${latestVersion.version}: ${compatibilityResult.errors?.join('; ')}`);
          }

          // Apply migrations if needed
          if (compatibilityResult.requiredMigrations?.length) {
            for (const migration of compatibilityResult.requiredMigrations) {
              const migrationStrategy = EventMigrationStrategyFactory.create(eventVersion.migrationStrategy);
              eventPayload.data = await migrationStrategy.apply(eventPayload.data, migration.steps);
            }
          }
        }
      }

      // Execute all handlers using the handler registry
      const results = await this.handlerRegistry.executeHandlers(eventPayload);
      
      const successCount = results.filter(r => r.success).length;
      const failureCount = results.filter(r => !r.success).length;
      
      if (failureCount > 0) {
        const errors = results.filter(r => !r.success).map(r => `${r.handlerName}: ${r.error}`);
        this.logger.error(`Event processing completed with ${failureCount} failures: ${eventType} (${eventId})`, {
          errors,
          successCount,
          failureCount
        });
        
        // Throw error if any handler failed
        throw new Error(`Event processing failed: ${errors.join('; ')}`);
      }

      this.logger.log(`Event processed successfully: ${eventType} (${eventId}) - ${successCount} handlers executed`);
    } catch (error) {
      this.logger.error(`Failed to process event: ${eventType} (${eventId})`, error);
      throw error;
    }
  }

  /**
   * Get service configuration
   */
  getConfig(): ResolvedEventStreamConfig {
    return this.config;
  }

  /**
   * Check if the service is initialized
   */
  isServiceInitialized(): boolean {
    return this.isInitialized;
  }

  /**
   * Get database health status
   */
  async getDatabaseHealth() {
    return await this.databaseAdapter.checkHealth();
  }

  /**
   * Get Redis health status
   */
  async getRedisHealth() {
    return await this.redisAdapter.checkHealth();
  }

  /**
   * Get database connection pool statistics
   */
  getDatabaseStats() {
    return this.databaseAdapter.getPoolStats();
  }

  /**
   * Get the database adapter instance
   * This allows access to database operations in later phases
   */
  getDatabaseAdapter(): DatabaseAdapter {
    return this.databaseAdapter;
  }

  /**
   * Get the Redis adapter instance
   * This allows access to Redis operations in later phases
   */
  getRedisAdapter(): RedisAdapter {
    return this.redisAdapter;
  }

  /**
   * Get the outbox manager instance
   * This allows access to outbox operations for advanced use cases
   */
  getOutboxManager(): OutboxManager {
    return this.outboxManager;
  }

  /**
   * Get the inbox manager instance
   * This allows access to inbox operations for advanced use cases
   */
  getInboxManager(): InboxManager {
    return this.inboxManager;
  }

  /**
   * Get the handler registry instance
   * This allows access to handler registration for advanced use cases
   */
  getHandlerRegistry(): HandlerRegistry {
    return this.handlerRegistry;
  }

  /**
   * Get the event consumer instance
   * This allows access to consumer operations for advanced use cases
   */
  getEventConsumer(): EventConsumer {
    return this.eventConsumer;
  }

  /**
   * Get the retry manager instance
   * This allows access to retry operations for advanced use cases
   */
  getRetryManager(): RetryManager {
    return this.retryManager;
  }

  /**
   * Get the DLQ manager instance
   * This allows access to DLQ operations for advanced use cases
   */
  getDLQManager(): DLQManager {
    return this.dlqManager;
  }

  /**
   * Get outbox statistics for monitoring
   */
  async getOutboxStats() {
    this.ensureInitialized();
    return await this.outboxManager.getOutboxStats();
  }

  /**
   * Retry a failed event from the outbox
   * @param eventId - The ID of the event to retry
   */
  async retryFailedEvent(eventId: string) {
    this.ensureInitialized();
    
    this.logger.debug(`Retrying failed event: ${eventId}`);
    
    const result = await this.outboxManager.retryFailedEvent(eventId);
    
    if (result.success) {
      this.logger.log(`Event retried successfully: ${eventId}`);
    } else {
      this.logger.error(`Failed to retry event: ${eventId}`, result.error);
      throw new Error(`Failed to retry event: ${result.error}`);
    }
    
    return result;
  }

  /**
   * Get pending events from outbox (useful for monitoring)
   * @param limit - Maximum number of events to return
   */
  async getPendingEvents(limit: number = 100) {
    this.ensureInitialized();
    return await this.outboxManager.getPendingEvents(limit);
  }

  /**
   * Get failed events that can be retried
   * @param limit - Maximum number of events to return
   */
  async getRetryableFailedEvents(limit: number = 50) {
    this.ensureInitialized();
    return await this.outboxManager.getRetryableFailedEvents(limit);
  }

  /**
   * Clean up old sent events from outbox
   * @param olderThanDays - Remove events older than this many days
   */
  async cleanupOutbox(olderThanDays: number = 7): Promise<number> {
    this.ensureInitialized();
    
    this.logger.log(`Cleaning up outbox events older than ${olderThanDays} days`);
    
    const deletedCount = await this.outboxManager.cleanupSentEvents(olderThanDays);
    
    this.logger.log(`Cleaned up ${deletedCount} sent events from outbox`);
    
    return deletedCount;
  }

  /**
   * Start the event consumer
   * This allows manual control over consumer lifecycle
   */
  async startConsumer(): Promise<void> {
    this.ensureInitialized();

    if (this.isConsumerRunning) {
      this.logger.warn('Event consumer is already running');
      return;
    }

    this.logger.log('Starting event consumer...');

    try {
      await this.eventConsumer.start(this.processEvent.bind(this));
      this.isConsumerRunning = true;
      this.logger.log('Event consumer started successfully');
    } catch (error) {
      this.logger.error('Failed to start event consumer', error);
      throw error;
    }
  }

  /**
   * Stop the event consumer
   * This allows manual control over consumer lifecycle
   */
  async stopConsumer(): Promise<void> {
    if (!this.isConsumerRunning) {
      this.logger.warn('Event consumer is not running');
      return;
    }

    this.logger.log('Stopping event consumer...');

    try {
      await this.eventConsumer.stop();
      this.isConsumerRunning = false;
      this.logger.log('Event consumer stopped successfully');
    } catch (error) {
      this.logger.error('Failed to stop event consumer', error);
      throw error;
    }
  }

  /**
   * Get consumer health status
   */
  getConsumerHealth(): ConsumerHealth {
    return this.eventConsumer.getHealth();
  }

  /**
   * Get consumer statistics
   */
  getConsumerStats(): ConsumerStats {
    return this.eventConsumer.getStats();
  }

  /**
   * Get handler statistics
   */
  getHandlerStats(eventType?: string): Map<string, HandlerStats> {
    return this.handlerRegistry.getHandlerStats(eventType);
  }

  /**
   * Check if consumer is running
   */
  isConsumerActive(): boolean {
    return this.isConsumerRunning && this.eventConsumer.isConsumerRunning();
  }

  /**
   * Get inbox statistics for monitoring
   */
  async getInboxStats() {
    this.ensureInitialized();
    return await this.inboxManager.getInboxStats();
  }

  /**
   * Clean up old processed events from inbox
   * @param olderThanDays - Remove events older than this many days
   */
  async cleanupInbox(olderThanDays: number = 7): Promise<number> {
    this.ensureInitialized();
    
    this.logger.log(`Cleaning up inbox events older than ${olderThanDays} days`);
    
    const deletedCount = await this.inboxManager.cleanupProcessedEvents(olderThanDays);
    
    this.logger.log(`Cleaned up ${deletedCount} processed events from inbox`);
    
    return deletedCount;
  }

  /**
   * Get retry statistics for monitoring
   */
  getRetryStats(): {
    policies: Record<string, import('./managers/retry.manager').RetryPolicy>;
    circuitBreakers: Record<string, {
      state: import('./managers/retry.manager').CircuitBreakerState;
      failureCount: number;
      requestCount: number;
    }>;
  } {
    this.ensureInitialized();
    return this.retryManager.getRetryStats();
  }

  /**
   * Reset circuit breaker for a specific operation
   * @param operationName - Name of the operation to reset
   */
  resetCircuitBreaker(operationName: string): void {
    this.ensureInitialized();
    this.retryManager.resetCircuitBreaker(operationName);
    this.logger.log(`Circuit breaker reset for operation: ${operationName}`);
  }

  /**
   * Reset all circuit breakers
   */
  resetAllCircuitBreakers(): void {
    this.ensureInitialized();
    this.retryManager.resetAllCircuitBreakers();
    this.logger.log('All circuit breakers have been reset');
  }

  /**
   * Get DLQ statistics for monitoring
   */
  async getDLQStats() {
    this.ensureInitialized();
    return await this.dlqManager.getDLQStats();
  }

  /**
   * Get events from DLQ with optional filtering
   * @param options - Query options for filtering DLQ events
   */
  async getDLQEvents(options: {
    count?: number;
    startId?: string;
    endId?: string;
  } = {}) {
    this.ensureInitialized();
    return await this.dlqManager.getDLQEvents(options);
  }

  /**
   * Get specific event from DLQ by event ID
   * @param eventId - The event ID to search for
   */
  async getDLQEventById(eventId: string) {
    this.ensureInitialized();
    return await this.dlqManager.getDLQEventById(eventId);
  }

  /**
   * Replay events from DLQ
   * @param options - Replay options and filters
   * @param replayHandler - Handler function to process replayed events
   */
  async replayFromDLQ(
    options: Parameters<typeof this.dlqManager.replayFromDLQ>[0] = {},
    replayHandler: Parameters<typeof this.dlqManager.replayFromDLQ>[1]
  ) {
    this.ensureInitialized();
    
    this.logger.log('Starting DLQ replay operation', options);
    
    const result = await this.dlqManager.replayFromDLQ(options, replayHandler);
    
    this.logger.log(`DLQ replay completed: ${result.eventsReplayed} successful, ${result.eventsFailed} failed`);
    
    return result;
  }

  /**
   * Purge events from DLQ
   * @param options - Purge options and filters
   */
  async purgeDLQ(options: Parameters<typeof this.dlqManager.purgeDLQ>[0] = {}) {
    this.ensureInitialized();
    
    this.logger.log('Starting DLQ purge operation', options);
    
    const result = await this.dlqManager.purgeDLQ(options);
    
    this.logger.log(`DLQ purge completed: ${result.eventsPurged} events purged`);
    
    if (result.errors.length > 0) {
      this.logger.warn(`DLQ purge had ${result.errors.length} errors`, result.errors);
    }
    
    return result;
  }

  /**
   * Clean up old events from DLQ based on retention policy
   */
  async cleanupDLQ(): Promise<number> {
    this.ensureInitialized();
    
    this.logger.log('Cleaning up DLQ based on retention policy');
    
    const deletedCount = await this.dlqManager.cleanupDLQ();
    
    this.logger.log(`Cleaned up ${deletedCount} events from DLQ`);
    
    return deletedCount;
  }

  /**
   * Check if event exists in DLQ
   * @param eventId - The event ID to check
   */
  async eventExistsInDLQ(eventId: string): Promise<boolean> {
    this.ensureInitialized();
    return await this.dlqManager.eventExistsInDLQ(eventId);
  }

  // ===== Archive Management Methods =====

  /**
   * Get archive manager instance
   */
  getArchiveManager(): ArchiveManager {
    this.ensureInitialized();
    return this.archiveManager;
  }

  /**
   * Get archive statistics
   */
  async getArchiveStats() {
    this.ensureInitialized();
    return await this.archiveManager.getArchiveStats();
  }

  /**
   * Query archived events
   */
  async queryArchivedEvents(options: Parameters<typeof this.archiveManager.queryArchivedEvents>[0] = {}) {
    this.ensureInitialized();
    return await this.archiveManager.queryArchivedEvents(options);
  }

  /**
   * Trigger manual archival
   */
  async triggerManualArchival() {
    this.ensureInitialized();
    return await this.archiveManager.triggerManualArchival();
  }

  /**
   * Clean up old archives
   */
  async cleanupOldArchives(): Promise<void> {
    this.ensureInitialized();
    await this.archiveManager.cleanupOldArchives();
  }

  /**
   * Get archival status
   */
  getArchiveStatus() {
    this.ensureInitialized();
    return this.archiveManager.getStatus();
  }

  // ===== Observability Methods =====

  /**
   * Get metrics collector instance
   */
  getMetricsCollector(): MetricsCollector {
    this.ensureInitialized();
    return this.metricsCollector;
  }

  /**
   * Get health checker instance
   */
  getHealthChecker(): HealthChecker {
    this.ensureInitialized();
    return this.healthChecker;
  }

  /**
   * Get Prometheus metrics
   */
  getPrometheusMetrics(): string {
    this.ensureInitialized();
    return this.metricsCollector.getPrometheusMetrics();
  }

  /**
   * Get metrics snapshot
   */
  getMetricsSnapshot() {
    this.ensureInitialized();
    return this.metricsCollector.getMetricsSnapshot();
  }

  /**
   * Get system health status
   */
  async getSystemHealth() {
    this.ensureInitialized();
    return await this.healthChecker.getSystemHealth();
  }

  /**
   * Get health endpoint response
   */
  async getHealthEndpointResponse() {
    this.ensureInitialized();
    return await this.healthChecker.getHealthEndpointResponse();
  }

  /**
   * Check if system is healthy
   */
  isHealthy(): boolean {
    this.ensureInitialized();
    return this.healthChecker.isHealthy();
  }

  // ===== Replay Methods =====

  /**
   * Get replay manager instance
   */
  getReplayManager(): ReplayManager {
    this.ensureInitialized();
    return this.replayManager;
  }

  /**
   * Get point-in-time recovery instance
   */
  getPointInTimeRecovery(): PointInTimeRecovery {
    this.ensureInitialized();
    return this.pointInTimeRecovery;
  }

  /**
   * Start event replay
   */
  async startReplay(options: ReplayOptions): Promise<ReplayHistory> {
    this.ensureInitialized();
    return await this.replayManager.startReplay(options);
  }

  /**
   * Get replay progress
   */
  getReplayProgress(replayId: string): ReplayProgress | undefined {
    this.ensureInitialized();
    return this.replayManager.getReplayProgress(replayId);
  }

  /**
   * Get replay history
   */
  getReplayHistory(replayId: string): ReplayHistory | undefined {
    this.ensureInitialized();
    return this.replayManager.getReplayHistory(replayId);
  }

  /**
   * Get all active replays
   */
  getActiveReplays(): Map<string, ReplayProgress> {
    this.ensureInitialized();
    return this.replayManager.getActiveReplays();
  }

  /**
   * Get all replay histories
   */
  async getAllReplayHistories(limit: number = 100): Promise<ReplayHistory[]> {
    this.ensureInitialized();
    return await this.replayManager.getAllReplayHistories(limit);
  }

  /**
   * Cancel replay
   */
  async cancelReplay(replayId: string): Promise<void> {
    this.ensureInitialized();
    await this.replayManager.cancelReplay(replayId);
  }

  /**
   * Pause replay
   */
  async pauseReplay(replayId: string): Promise<void> {
    this.ensureInitialized();
    await this.replayManager.pauseReplay(replayId);
  }

  /**
   * Resume replay
   */
  async resumeReplay(replayId: string): Promise<void> {
    this.ensureInitialized();
    await this.replayManager.resumeReplay(replayId);
  }

  /**
   * Clean up old replay histories
   */
  async cleanupReplayHistories(olderThanDays: number = 30): Promise<number> {
    this.ensureInitialized();
    return await this.replayManager.cleanupReplayHistories(olderThanDays);
  }

  /**
   * Recover to a point in time
   */
  async recoverToPointInTime(options: PointInTimeRecoveryOptions): Promise<PointInTimeRecoveryResult> {
    this.ensureInitialized();
    return await this.pointInTimeRecovery.recover(options);
  }

  /**
   * Get recovery history
   */
  async getRecoveryHistory(recoveryId: string): Promise<PointInTimeRecoveryResult | null> {
    this.ensureInitialized();
    return await this.pointInTimeRecovery.getRecoveryHistory(recoveryId);
  }

  /**
   * Get all recovery histories
   */
  async getAllRecoveryHistories(limit: number = 100): Promise<PointInTimeRecoveryResult[]> {
    this.ensureInitialized();
    return await this.pointInTimeRecovery.getAllRecoveryHistories(limit);
  }

  /**
   * Clean up old recovery histories
   */
  async cleanupRecoveryHistories(olderThanDays: number = 30): Promise<number> {
    this.ensureInitialized();
    return await this.pointInTimeRecovery.cleanupRecoveryHistories(olderThanDays);
  }

  // ===== Schema Registry Methods =====

  /**
   * Get schema registry instance
   */
  getSchemaRegistry(): SchemaRegistry {
    this.ensureInitialized();
    return this.schemaRegistry;
  }

  /**
   * Register a new event schema
   */
  async registerEventSchema(
    eventType: string,
    schema: JSONSchema7,
    options: Omit<SchemaRegistrationOptions, 'name' | 'namespace' | 'schema'> = {}
  ): Promise<SchemaMetadata> {
    this.ensureInitialized();

    const [namespace, name] = eventType.split('.');
    if (!namespace || !name) {
      throw new Error('Invalid event type format. Expected format: "namespace.name"');
    }

    return await this.schemaRegistry.registerSchema({
      namespace,
      name,
      schema,
      ...options,
    });
  }

  /**
   * Update an existing event schema
   */
  async updateEventSchema(
    eventType: string,
    schema: JSONSchema7,
    options: Omit<SchemaUpdateOptions, 'schema'> = {}
  ): Promise<SchemaMetadata> {
    this.ensureInitialized();

    const [namespace, name] = eventType.split('.');
    if (!namespace || !name) {
      throw new Error('Invalid event type format. Expected format: "namespace.name"');
    }

    return await this.schemaRegistry.updateSchema(namespace, name, {
      schema,
      ...options,
    });
  }

  /**
   * Get event schema
   */
  async getEventSchema(eventType: string, version?: number): Promise<SchemaVersion | null> {
    this.ensureInitialized();

    const [namespace, name] = eventType.split('.');
    if (!namespace || !name) {
      throw new Error('Invalid event type format. Expected format: "namespace.name"');
    }

    return await this.schemaRegistry.findSchemaVersion(namespace, name, version || 0);
  }

  /**
   * Validate event data against schema
   */
  async validateEventData<T = any>(
    eventType: string,
    data: T,
    version?: number
  ): Promise<SchemaValidationResult> {
    this.ensureInitialized();

    const [namespace, name] = eventType.split('.');
    if (!namespace || !name) {
      throw new Error('Invalid event type format. Expected format: "namespace.name"');
    }

    return await this.schemaRegistry.validateData(namespace, name, data, version);
  }

  /**
   * Create migration plan between schema versions
   */
  async createSchemaMigrationPlan(
    eventType: string,
    sourceVersion: number,
    targetVersion: number
  ): Promise<SchemaMigrationPlan> {
    this.ensureInitialized();

    const [namespace, name] = eventType.split('.');
    if (!namespace || !name) {
      throw new Error('Invalid event type format. Expected format: "namespace.name"');
    }

    return await this.schemaRegistry.createMigrationPlan(namespace, name, sourceVersion, targetVersion);
  }

  /**
   * Gracefully shutdown the service
   */
  async shutdown(): Promise<void> {
    if (!this.isInitialized) {
      return;
    }

    this.logger.log('Shutting down EventStreamService...');

    try {
      // Phase 6 - Stop consumers gracefully
      if (this.isConsumerRunning) {
        this.logger.log('Stopping event consumer...');
        await this.stopConsumer();
        this.logger.log('Event consumer stopped successfully');
      }
      
      // Phase 3 - Close Redis connections
      this.logger.log('Shutting down Redis adapter...');
      await this.redisAdapter.onApplicationShutdown();
      this.logger.log('Redis adapter shut down successfully');
      
      // Phase 2 - Close database connections
      this.logger.log('Shutting down database adapter...');
      await this.databaseAdapter.onApplicationShutdown();
      this.logger.log('Database adapter shut down successfully');
      
      this.isInitialized = false;
      this.logger.log('EventStreamService shut down successfully');
    } catch (error) {
      this.logger.error('Error during EventStreamService shutdown', error);
      throw error;
    }
  }

  /**
   * Ensure the service is initialized before operations
   */
  private ensureInitialized(): void {
    if (!this.isInitialized) {
      throw new Error('EventStreamService is not initialized. Call initialize() first.');
    }
  }
}
