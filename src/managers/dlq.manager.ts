import { Injectable, Logger } from '@nestjs/common';
import { DatabaseAdapter, DatabaseResult } from '../adapters/db.adapter';
import { RedisAdapter, RedisResult, StreamMessage } from '../adapters/redis.adapter';
import { ResolvedEventStreamConfig } from '../interfaces';
import { RetryManager, ErrorType } from './retry.manager';

/**
 * DLQ event data structure
 */
export interface DLQEventData {
  /** Original event ID */
  eventId: string;
  /** Event type */
  eventType: string;
  /** Original event payload */
  payload: Record<string, any>;
  /** Correlation ID for tracing */
  correlationId?: string | undefined;
  /** Trace ID for distributed tracing */
  traceId?: string | undefined;
  /** Event version */
  version?: string | undefined;
  /** Source service that published the event */
  sourceService?: string | undefined;
  /** Original event creation timestamp */
  originalCreatedAt: string;
  /** Timestamp when event was sent to DLQ */
  dlqTimestamp: string;
  /** Final error message */
  errorMessage: string;
  /** Error type classification */
  errorType: ErrorType;
  /** Total retry attempts made */
  retryAttempts: number;
  /** Maximum retry attempts allowed */
  maxRetryAttempts: number;
  /** Processing context (outbox/inbox) */
  processingContext: 'outbox' | 'inbox';
  /** Additional metadata */
  metadata?: Record<string, any> | undefined;
}

/**
 * DLQ event with Redis message information
 */
export interface DLQEvent extends DLQEventData {
  /** Redis message ID */
  dlqMessageId: string;
}

/**
 * DLQ statistics
 */
export interface DLQStats {
  /** Total events in DLQ */
  totalEvents: number;
  /** Events by error type */
  eventsByErrorType: Record<ErrorType, number>;
  /** Events by processing context */
  eventsByContext: {
    outbox: number;
    inbox: number;
  };
  /** Events by source service */
  eventsByService: Record<string, number>;
  /** Recent events (last 24 hours) */
  recentEvents: number;
  /** Oldest event timestamp */
  oldestEventTimestamp?: string;
  /** Newest event timestamp */
  newestEventTimestamp?: string;
}

/**
 * DLQ replay options
 */
export interface DLQReplayOptions {
  /** Maximum number of events to replay */
  maxEvents?: number;
  /** Filter by error type */
  errorType?: ErrorType;
  /** Filter by event type */
  eventType?: string;
  /** Filter by source service */
  sourceService?: string;
  /** Filter by processing context */
  processingContext?: 'outbox' | 'inbox';
  /** Skip events newer than this timestamp */
  olderThan?: Date;
  /** Skip events older than this timestamp */
  newerThan?: Date;
}

/**
 * DLQ replay result
 */
export interface DLQReplayResult {
  /** Number of events found for replay */
  eventsFound: number;
  /** Number of events successfully replayed */
  eventsReplayed: number;
  /** Number of events that failed replay */
  eventsFailed: number;
  /** List of failed event IDs with errors */
  failures: Array<{
    eventId: string;
    error: string;
  }>;
}

/**
 * DLQ purge options
 */
export interface DLQPurgeOptions {
  /** Filter by error type */
  errorType?: ErrorType;
  /** Filter by event type */
  eventType?: string;
  /** Filter by source service */
  sourceService?: string;
  /** Filter by processing context */
  processingContext?: 'outbox' | 'inbox';
  /** Purge events older than this timestamp */
  olderThan?: Date;
  /** Skip events older than this timestamp */
  newerThan?: Date;
  /** Maximum number of events to purge */
  maxEvents?: number;
}

/**
 * DLQ purge result
 */
export interface DLQPurgeResult {
  /** Number of events purged */
  eventsPurged: number;
  /** Any errors encountered during purge */
  errors: string[];
}

/**
 * Dead Letter Queue manager handles permanently failed events
 * 
 * Flow:
 * 1. Receive failed events from outbox/inbox managers
 * 2. Store in DLQ Redis stream with failure metadata
 * 3. Provide inspection and monitoring capabilities
 * 4. Allow manual replay of events from DLQ
 * 5. Support retention and cleanup policies
 */
@Injectable()
export class DLQManager {
  private readonly logger = new Logger(DLQManager.name);
  private readonly serviceName: string;
  private readonly dlqStreamName: string;
  private readonly retentionMs: number;

  constructor(
    private readonly config: ResolvedEventStreamConfig,
    private readonly redisAdapter: RedisAdapter,
    private readonly retryManager: RetryManager,
  ) {
    this.serviceName = config.serviceName;
    this.dlqStreamName = redisAdapter.getStreamNames().dlq;
    // Default retention: 30 days
    this.retentionMs = (config.dlq?.retentionDays || 30) * 24 * 60 * 60 * 1000;
  }

  /**
   * Send a failed event to the DLQ
   */
  async sendToDLQ(
    eventData: Omit<DLQEventData, 'dlqTimestamp'>,
    additionalMetadata?: Record<string, any>
  ): Promise<RedisResult<string>> {
    const dlqTimestamp = new Date().toISOString();
    
    const dlqEvent: DLQEventData = {
      ...eventData,
      dlqTimestamp,
      metadata: {
        ...eventData.metadata,
        ...additionalMetadata,
      },
    };

    this.logger.warn(`Sending event to DLQ: ${eventData.eventType} (${eventData.eventId})`, {
      errorType: eventData.errorType,
      retryAttempts: eventData.retryAttempts,
      errorMessage: eventData.errorMessage,
      processingContext: eventData.processingContext,
    });

    // Prepare Redis stream fields
    const streamFields = {
      eventId: dlqEvent.eventId,
      eventType: dlqEvent.eventType,
      payload: JSON.stringify(dlqEvent.payload),
      correlationId: dlqEvent.correlationId || '',
      traceId: dlqEvent.traceId || '',
      version: dlqEvent.version || '1.0',
      sourceService: dlqEvent.sourceService || this.serviceName,
      originalCreatedAt: dlqEvent.originalCreatedAt,
      dlqTimestamp: dlqEvent.dlqTimestamp,
      errorMessage: dlqEvent.errorMessage,
      errorType: dlqEvent.errorType,
      retryAttempts: dlqEvent.retryAttempts.toString(),
      maxRetryAttempts: dlqEvent.maxRetryAttempts.toString(),
      processingContext: dlqEvent.processingContext,
      metadata: JSON.stringify(dlqEvent.metadata || {}),
    };

    try {
      const result = await this.redisAdapter.addToStream(
        this.dlqStreamName,
        streamFields,
        this.config.dlq?.maxStreamLength || 10000
      );

      if (result.success) {
        this.logger.log(`Event successfully sent to DLQ: ${eventData.eventType} (${eventData.eventId}), DLQ ID: ${result.data}`);
      } else {
        this.logger.error(`Failed to send event to DLQ: ${eventData.eventType} (${eventData.eventId})`, result.error);
      }

      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Unexpected error sending event to DLQ: ${eventData.eventType} (${eventData.eventId})`, error);
      
      return {
        success: false,
        error: errorMessage,
      };
    }
  }

  /**
   * Get events from DLQ with optional filtering
   */
  async getDLQEvents(
    options: {
      count?: number;
      startId?: string;
      endId?: string;
    } = {}
  ): Promise<RedisResult<DLQEvent[]>> {
    const { count = 100, startId = '-', endId = '+' } = options;

    try {
      const result = await this.redisAdapter.readStreamRange(this.dlqStreamName, {
        count,
        startId,
        endId,
      });

      if (!result.success) {
        return {
          success: false,
          error: result.error
        };
      }

      const dlqEvents: DLQEvent[] = (result.data || []).map((message: StreamMessage) => {
        const fields = message.message;
        
        return {
          dlqMessageId: message.id,
          eventId: fields.eventId,
          eventType: fields.eventType,
          payload: JSON.parse(fields.payload || '{}'),
          correlationId: fields.correlationId || undefined,
          traceId: fields.traceId || undefined,
          version: fields.version,
          sourceService: fields.sourceService,
          originalCreatedAt: fields.originalCreatedAt,
          dlqTimestamp: fields.dlqTimestamp,
          errorMessage: fields.errorMessage,
          errorType: fields.errorType as ErrorType,
          retryAttempts: parseInt(fields.retryAttempts || '0'),
          maxRetryAttempts: parseInt(fields.maxRetryAttempts || '0'),
          processingContext: fields.processingContext as 'outbox' | 'inbox',
          metadata: JSON.parse(fields.metadata || '{}'),
        };
      });

      return {
        success: true,
        data: dlqEvents,
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error('Failed to get DLQ events', error);
      
      return {
        success: false,
        error: errorMessage,
      };
    }
  }

  /**
   * Get DLQ statistics
   */
  async getDLQStats(): Promise<DLQStats> {
    try {
      // Get stream info
      const streamInfo = await this.redisAdapter.getStreamInfo(this.dlqStreamName);
      const totalEvents = streamInfo.success ? streamInfo.data?.length || 0 : 0;

      if (totalEvents === 0) {
        return {
          totalEvents: 0,
          eventsByErrorType: {} as Record<ErrorType, number>,
          eventsByContext: { outbox: 0, inbox: 0 },
          eventsByService: {},
          recentEvents: 0,
        };
      }

      // Get recent events to calculate statistics
      const eventsResult = await this.getDLQEvents({ count: Math.min(totalEvents, 1000) });
      const events = eventsResult.success ? eventsResult.data || [] : [];

      const stats: DLQStats = {
        totalEvents,
        eventsByErrorType: {} as Record<ErrorType, number>,
        eventsByContext: { outbox: 0, inbox: 0 },
        eventsByService: {},
        recentEvents: 0,
      };

      // Calculate statistics from sample
      const now = new Date();
      const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);

      events.forEach(event => {
        // Count by error type
        stats.eventsByErrorType[event.errorType] = (stats.eventsByErrorType[event.errorType] || 0) + 1;

        // Count by processing context
        stats.eventsByContext[event.processingContext]++;

        // Count by source service
        if (event.sourceService) {
          stats.eventsByService[event.sourceService] = (stats.eventsByService[event.sourceService] || 0) + 1;
        }

        // Count recent events
        const eventTime = new Date(event.dlqTimestamp);
        if (eventTime > oneDayAgo) {
          stats.recentEvents++;
        }

        // Track oldest and newest timestamps
        if (!stats.oldestEventTimestamp || event.dlqTimestamp < stats.oldestEventTimestamp) {
          stats.oldestEventTimestamp = event.dlqTimestamp;
        }
        if (!stats.newestEventTimestamp || event.dlqTimestamp > stats.newestEventTimestamp) {
          stats.newestEventTimestamp = event.dlqTimestamp;
        }
      });

      return stats;
    } catch (error) {
      this.logger.error('Failed to get DLQ statistics', error);
      return {
        totalEvents: 0,
        eventsByErrorType: {} as Record<ErrorType, number>,
        eventsByContext: { outbox: 0, inbox: 0 },
        eventsByService: {},
        recentEvents: 0,
      };
    }
  }

  /**
   * Replay events from DLQ
   */
  async replayFromDLQ(
    options: DLQReplayOptions = {},
    replayHandler: (event: DLQEvent) => Promise<boolean>
  ): Promise<DLQReplayResult> {
    this.logger.log('Starting DLQ replay operation', options);

    const result: DLQReplayResult = {
      eventsFound: 0,
      eventsReplayed: 0,
      eventsFailed: 0,
      failures: [],
    };

    try {
      // Get events from DLQ
      const eventsResult = await this.getDLQEvents({
        count: options.maxEvents || 1000,
      });

      if (!eventsResult.success) {
        throw new Error(`Failed to get DLQ events: ${eventsResult.error}`);
      }

      let events = eventsResult.data || [];
      result.eventsFound = events.length;

      // Apply filters
      events = this.filterDLQEvents(events, options);
      
      this.logger.log(`Found ${events.length} events matching filters for replay`);

      // Replay filtered events
      for (const event of events) {
        try {
          this.logger.debug(`Replaying event: ${event.eventType} (${event.eventId})`);
          
          const success = await replayHandler(event);
          
          if (success) {
            result.eventsReplayed++;
            
            // Remove from DLQ after successful replay
            await this.removeFromDLQ(event.dlqMessageId);
            
            this.logger.log(`Successfully replayed event: ${event.eventType} (${event.eventId})`);
          } else {
            result.eventsFailed++;
            result.failures.push({
              eventId: event.eventId,
              error: 'Replay handler returned false',
            });
            
            this.logger.warn(`Replay handler failed for event: ${event.eventType} (${event.eventId})`);
          }
        } catch (error) {
          result.eventsFailed++;
          const errorMessage = error instanceof Error ? error.message : String(error);
          result.failures.push({
            eventId: event.eventId,
            error: errorMessage,
          });
          
          this.logger.error(`Failed to replay event: ${event.eventType} (${event.eventId})`, error);
        }
      }

      this.logger.log(`DLQ replay completed: ${result.eventsReplayed} successful, ${result.eventsFailed} failed`);
      
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error('DLQ replay operation failed', error);
      
      result.failures.push({
        eventId: 'unknown',
        error: errorMessage,
      });
      
      return result;
    }
  }

  /**
   * Purge events from DLQ
   */
  async purgeDLQ(options: DLQPurgeOptions = {}): Promise<DLQPurgeResult> {
    this.logger.log('Starting DLQ purge operation', options);

    const result: DLQPurgeResult = {
      eventsPurged: 0,
      errors: [],
    };

    try {
      // Get events from DLQ
      const eventsResult = await this.getDLQEvents({
        count: options.maxEvents || 1000,
      });

      if (!eventsResult.success) {
        result.errors.push(`Failed to get DLQ events: ${eventsResult.error}`);
        return result;
      }

      let events = eventsResult.data || [];

      // Apply filters
      events = this.filterDLQEvents(events, options);
      
      this.logger.log(`Found ${events.length} events matching filters for purge`);

      // Purge filtered events
      for (const event of events) {
        try {
          await this.removeFromDLQ(event.dlqMessageId);
          result.eventsPurged++;
          
          this.logger.debug(`Purged event from DLQ: ${event.eventType} (${event.eventId})`);
        } catch (error) {
          const errorMessage = error instanceof Error ? error.message : String(error);
          result.errors.push(`Failed to purge event ${event.eventId}: ${errorMessage}`);
          
          this.logger.error(`Failed to purge event from DLQ: ${event.eventType} (${event.eventId})`, error);
        }
      }

      this.logger.log(`DLQ purge completed: ${result.eventsPurged} events purged, ${result.errors.length} errors`);
      
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error('DLQ purge operation failed', error);
      
      result.errors.push(errorMessage);
      return result;
    }
  }

  /**
   * Remove specific event from DLQ by message ID
   */
  async removeFromDLQ(messageId: string): Promise<RedisResult<number>> {
    try {
      const result = await this.redisAdapter.deleteFromStream(this.dlqStreamName, [messageId]);
      
      if (result.success) {
        this.logger.debug(`Removed message from DLQ: ${messageId}`);
      } else {
        this.logger.error(`Failed to remove message from DLQ: ${messageId}`, result.error);
      }
      
      return result;
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Unexpected error removing message from DLQ: ${messageId}`, error);
      
      return {
        success: false,
        error: errorMessage,
      };
    }
  }

  /**
   * Clean up old events from DLQ based on retention policy
   */
  async cleanupDLQ(): Promise<number> {
    this.logger.log('Starting DLQ cleanup operation');

    try {
      const cutoffTime = new Date(Date.now() - this.retentionMs);
      
      const purgeResult = await this.purgeDLQ({
        olderThan: cutoffTime,
        maxEvents: 10000, // Process in batches
      });

      this.logger.log(`DLQ cleanup completed: ${purgeResult.eventsPurged} events purged`);
      
      if (purgeResult.errors.length > 0) {
        this.logger.warn(`DLQ cleanup had ${purgeResult.errors.length} errors`, purgeResult.errors);
      }

      return purgeResult.eventsPurged;
    } catch (error) {
      this.logger.error('DLQ cleanup operation failed', error);
      return 0;
    }
  }

  /**
   * Get specific event from DLQ by event ID
   */
  async getDLQEventById(eventId: string): Promise<DLQEvent | null> {
    try {
      const eventsResult = await this.getDLQEvents({ count: 1000 });
      
      if (!eventsResult.success || !eventsResult.data) {
        return null;
      }

      const event = eventsResult.data.find(e => e.eventId === eventId);
      return event || null;
    } catch (error) {
      this.logger.error(`Failed to get DLQ event by ID: ${eventId}`, error);
      return null;
    }
  }

  /**
   * Check if event exists in DLQ
   */
  async eventExistsInDLQ(eventId: string): Promise<boolean> {
    const event = await this.getDLQEventById(eventId);
    return event !== null;
  }

  /**
   * Filter DLQ events based on options
   */
  private filterDLQEvents(
    events: DLQEvent[],
    options: DLQReplayOptions | DLQPurgeOptions
  ): DLQEvent[] {
    return events.filter(event => {
      // Filter by error type
      if (options.errorType && event.errorType !== options.errorType) {
        return false;
      }

      // Filter by event type
      if (options.eventType && event.eventType !== options.eventType) {
        return false;
      }

      // Filter by source service
      if (options.sourceService && event.sourceService !== options.sourceService) {
        return false;
      }

      // Filter by processing context
      if (options.processingContext && event.processingContext !== options.processingContext) {
        return false;
      }

      // Filter by date range
      const eventTime = new Date(event.dlqTimestamp);
      
      if (options.olderThan && eventTime >= options.olderThan) {
        return false;
      }

      if (options.newerThan && eventTime <= options.newerThan) {
        return false;
      }

      return true;
    });
  }

  /**
   * Get DLQ stream name
   */
  getDLQStreamName(): string {
    return this.dlqStreamName;
  }

  /**
   * Get DLQ retention period in milliseconds
   */
  getRetentionMs(): number {
    return this.retentionMs;
  }
}
