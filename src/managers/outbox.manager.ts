import { Injectable, Logger } from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';
import { DatabaseAdapter, DatabaseResult, OutboxEventRecord } from '../adapters/db.adapter';
import { RedisAdapter } from '../adapters/redis.adapter';
import { EventStatus, TABLE_NAMES } from '../schemas/database.schema';
import { ResolvedEventStreamConfig } from '../interfaces';
import { RetryManager } from './retry.manager';
import { DLQManager } from './dlq.manager';

/**
 * Outbox event creation data
 */
export interface OutboxEventData {
  eventId: string;
  eventType: string;
  payload: Record<string, any>;
  correlationId?: string | undefined;
  traceId?: string | undefined;
  version?: string | undefined;
}

/**
 * Outbox publish result
 */
export interface OutboxPublishResult {
  success: boolean;
  eventId?: string;
  error?: string | undefined;
  retryable?: boolean | undefined;
}

/**
 * Outbox manager handles the outbox pattern for reliable event publishing
 * 
 * Flow:
 * 1. Insert event into outbox_events table (status = pending)
 * 2. Publish to Redis Stream 
 * 3. Update outbox_events table (status = sent)
 * 
 * This ensures no events are lost if the service crashes during publishing
 */
@Injectable()
export class OutboxManager {
  private readonly logger = new Logger(OutboxManager.name);
  private readonly serviceName: string;
  private readonly streamName: string;
  private readonly maxRetries: number;

  constructor(
    private readonly config: ResolvedEventStreamConfig,
    private readonly databaseAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
    private readonly retryManager?: RetryManager,
    private readonly dlqManager?: DLQManager,
  ) {
    this.serviceName = config.serviceName;
    this.streamName = redisAdapter.getStreamNames().events;
    this.maxRetries = config.outbox?.maxRetries || 3;
  }

  /**
   * Publish an event using the outbox pattern
   * This is the main entry point for event publishing
   */
  async publishEvent(eventData: OutboxEventData): Promise<OutboxPublishResult> {
    const { eventId, eventType } = eventData;

    this.logger.debug(`Publishing event via outbox: ${eventType} (${eventId})`);

    try {
      // Step 1: Insert event into outbox with pending status
      const insertResult = await this.insertEventToOutbox(eventData);
      if (!insertResult.success) {
        return {
          success: false,
          error: `Failed to insert event to outbox: ${insertResult.error}`,
          retryable: false
        };
      }

      // Step 2: Publish to Redis Stream
      const publishResult = await this.publishToRedis(eventData);
      if (!publishResult.success) {
        // Mark as failed in outbox
        await this.updateEventStatus(eventId, EventStatus.FAILED, publishResult.error);
        
        return {
          success: false,
          eventId,
          error: publishResult.error || undefined,
          retryable: publishResult.retryable || undefined
        };
      }

      // Step 3: Mark as sent in outbox
      const updateResult = await this.updateEventStatus(eventId, EventStatus.SENT);
      if (!updateResult.success) {
        this.logger.warn(`Event published to Redis but failed to update status: ${eventId}`);
        // Don't return error here as the event was actually published
      }

      this.logger.debug(`Event published successfully via outbox: ${eventType} (${eventId})`);

      return {
        success: true,
        eventId
      };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Unexpected error during outbox publish: ${eventType} (${eventId})`, error);

      // Try to mark as failed in outbox
      try {
        await this.updateEventStatus(eventId, EventStatus.FAILED, errorMessage);
      } catch (updateError) {
        this.logger.error(`Failed to update event status after error: ${eventId}`, updateError);
      }

      return {
        success: false,
        eventId,
        error: errorMessage,
        retryable: true
      };
    }
  }

  /**
   * Insert event into outbox table with pending status
   */
  private async insertEventToOutbox(eventData: OutboxEventData): Promise<DatabaseResult> {
    const { eventId, eventType, payload, correlationId, traceId, version } = eventData;

    // Create the full payload with metadata
    const fullPayload = {
      data: payload,
      metadata: {
        correlationId,
        traceId,
        version: version || '1.0',
        sourceService: this.serviceName,
        createdAt: new Date().toISOString()
      }
    };

    const query = `
      INSERT INTO ${TABLE_NAMES.OUTBOX_EVENTS} 
      (event_id, event_type, payload, status, service_name, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
    `;

    const params = [
      eventId,
      eventType,
      JSON.stringify(fullPayload),
      EventStatus.PENDING,
      this.serviceName
    ];

    this.logger.debug(`Inserting event to outbox: ${eventType} (${eventId})`);

    const result = await this.databaseAdapter.query(query, params);
    
    if (!result.success) {
      this.logger.error(`Failed to insert event to outbox: ${eventType} (${eventId})`, result.error);
    } else {
      this.logger.debug(`Event inserted to outbox successfully: ${eventType} (${eventId})`);
    }

    return result;
  }

  /**
   * Publish event to Redis Stream
   */
  private async publishToRedis(eventData: OutboxEventData): Promise<OutboxPublishResult> {
    const { eventId, eventType, payload, correlationId, traceId, version } = eventData;

    // Prepare Redis stream fields
    const streamFields = {
      eventId,
      eventType,
      payload: JSON.stringify(payload),
      correlationId: correlationId || '',
      traceId: traceId || '',
      version: version || '1.0',
      sourceService: this.serviceName,
      createdAt: new Date().toISOString()
    };

    this.logger.debug(`Publishing to Redis stream: ${this.streamName}`);

    const result = await this.redisAdapter.addToStream(
      this.streamName,
      streamFields,
      this.config.redis.maxStreamLength
    );

    if (!result.success) {
      const isRetryable = this.isRetryableRedisError(result.error);
      
      this.logger.error(`Failed to publish to Redis stream: ${eventType} (${eventId})`, {
        error: result.error,
        retryable: isRetryable
      });

      return {
        success: false,
        error: result.error || undefined,
        retryable: isRetryable
      };
    }

    this.logger.debug(`Event published to Redis successfully: ${eventType} (${eventId}), messageId: ${result.data}`);

    return {
      success: true,
      eventId
    };
  }

  /**
   * Update event status in outbox table
   */
  private async updateEventStatus(
    eventId: string, 
    status: EventStatus, 
    errorMessage?: string
  ): Promise<DatabaseResult> {
    let query: string;
    let params: any[];

    if (status === EventStatus.FAILED && errorMessage) {
      query = `
        UPDATE ${TABLE_NAMES.OUTBOX_EVENTS} 
        SET status = $1, error_message = $2, updated_at = NOW(), retry_count = retry_count + 1, last_retry_at = NOW()
        WHERE event_id = $3
      `;
      params = [status, errorMessage, eventId];
    } else {
      query = `
        UPDATE ${TABLE_NAMES.OUTBOX_EVENTS} 
        SET status = $1, updated_at = NOW()
        WHERE event_id = $2
      `;
      params = [status, eventId];
    }

    this.logger.debug(`Updating event status to ${status}: ${eventId}`);

    const result = await this.databaseAdapter.query(query, params);

    if (!result.success) {
      this.logger.error(`Failed to update event status: ${eventId}`, result.error);
    } else if (result.affectedRows === 0) {
      this.logger.warn(`No rows affected when updating event status: ${eventId}`);
    } else {
      this.logger.debug(`Event status updated successfully: ${eventId} -> ${status}`);
    }

    return result;
  }

  /**
   * Determine if a Redis error is retryable
   */
  private isRetryableRedisError(error?: string): boolean {
    if (!error) return false;

    const retryableErrors = [
      'connection', 
      'timeout', 
      'network', 
      'temporarily unavailable',
      'max clients reached',
      'loading'
    ];

    const errorLower = error.toLowerCase();
    return retryableErrors.some(retryableError => errorLower.includes(retryableError));
  }

  /**
   * Get pending events from outbox (for recovery/retry scenarios)
   */
  async getPendingEvents(limit: number = 100): Promise<DatabaseResult<OutboxEventRecord[]>> {
    const query = `
      SELECT * FROM ${TABLE_NAMES.OUTBOX_EVENTS}
      WHERE status = $1 AND service_name = $2
      ORDER BY created_at ASC
      LIMIT $3
    `;

    const params = [EventStatus.PENDING, this.serviceName, limit];

    this.logger.debug(`Fetching pending events for service: ${this.serviceName}`);

    const result = await this.databaseAdapter.query<OutboxEventRecord>(query, params);

    if (result.success) {
      this.logger.debug(`Found ${result.data?.length || 0} pending events`);
    } else {
      this.logger.error('Failed to fetch pending events', result.error);
    }

    return result;
  }

  /**
   * Get failed events that can be retried
   */
  async getRetryableFailedEvents(limit: number = 50): Promise<DatabaseResult<OutboxEventRecord[]>> {
    const query = `
      SELECT * FROM ${TABLE_NAMES.OUTBOX_EVENTS}
      WHERE status = $1 AND service_name = $2 AND retry_count < $3
      ORDER BY last_retry_at ASC
      LIMIT $4
    `;

    const params = [EventStatus.FAILED, this.serviceName, this.maxRetries, limit];

    this.logger.debug(`Fetching retryable failed events for service: ${this.serviceName}`);

    const result = await this.databaseAdapter.query<OutboxEventRecord>(query, params);

    if (result.success) {
      this.logger.debug(`Found ${result.data?.length || 0} retryable failed events`);
    } else {
      this.logger.error('Failed to fetch retryable failed events', result.error);
    }

    return result;
  }

  /**
   * Retry a failed event
   */
  async retryFailedEvent(eventId: string): Promise<OutboxPublishResult> {
    this.logger.debug(`Retrying failed event: ${eventId}`);

    try {
      // First, get the event from outbox
      const query = `
        SELECT * FROM ${TABLE_NAMES.OUTBOX_EVENTS}
        WHERE event_id = $1 AND service_name = $2
      `;

      const result = await this.databaseAdapter.query<OutboxEventRecord>(query, [eventId, this.serviceName]);

      if (!result.success || !result.data || result.data.length === 0) {
        return {
          success: false,
          error: `Event not found in outbox: ${eventId}`,
          retryable: false
        };
      }

      const event = result.data[0];

      // Check if event can be retried
      if (event.retry_count >= this.maxRetries) {
        // Send to DLQ if max retries exceeded
        await this.sendEventToDLQ(event, `Maximum retry attempts (${this.maxRetries}) exceeded`);
        
        return {
          success: false,
          error: `Event has exceeded max retry attempts and was sent to DLQ: ${eventId}`,
          retryable: false
        };
      }

      // Parse the payload to extract original event data
      let parsedPayload: any;
      try {
        parsedPayload = JSON.parse(event.payload as any);
      } catch (parseError) {
        return {
          success: false,
          error: `Failed to parse event payload: ${eventId}`,
          retryable: false
        };
      }

      // Extract event data for retry
      const eventData: OutboxEventData = {
        eventId: event.event_id,
        eventType: event.event_type,
        payload: parsedPayload.data,
        correlationId: parsedPayload.metadata?.correlationId,
        traceId: parsedPayload.metadata?.traceId,
        version: parsedPayload.metadata?.version
      };

      // Reset status to pending
      await this.updateEventStatus(eventId, EventStatus.PENDING);

      // Attempt to publish again
      return await this.publishEvent(eventData);

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Unexpected error during event retry: ${eventId}`, error);

      return {
        success: false,
        eventId,
        error: errorMessage,
        retryable: true
      };
    }
  }

  /**
   * Send failed event to DLQ
   */
  private async sendEventToDLQ(event: OutboxEventRecord, errorMessage: string): Promise<void> {
    if (!this.dlqManager || !this.config.dlq?.enabled) {
      this.logger.warn(`DLQ is disabled, cannot send failed event to DLQ: ${event.event_id}`);
      return;
    }

    try {
      // Parse the payload to extract original event data
      let parsedPayload: any;
      try {
        parsedPayload = JSON.parse(event.payload as any);
      } catch (parseError) {
        this.logger.error(`Failed to parse event payload for DLQ: ${event.event_id}`, parseError);
        return;
      }

      // Classify error type
      const errorType = this.retryManager?.classifyError(errorMessage) || 'unknown' as any;

      // Send to DLQ
      await this.dlqManager.sendToDLQ({
        eventId: event.event_id,
        eventType: event.event_type,
        payload: parsedPayload.data,
        correlationId: parsedPayload.metadata?.correlationId,
        traceId: parsedPayload.metadata?.traceId,
        version: parsedPayload.metadata?.version,
        sourceService: this.serviceName,
        originalCreatedAt: event.created_at.toISOString(),
        errorMessage,
        errorType,
        retryAttempts: event.retry_count || 0,
        maxRetryAttempts: this.maxRetries,
        processingContext: 'outbox',
        metadata: {
          outboxCreatedAt: event.created_at.toISOString(),
          outboxUpdatedAt: event.updated_at?.toISOString(),
          outboxServiceName: event.service_name,
        },
      });

      this.logger.log(`Event sent to DLQ: ${event.event_type} (${event.event_id})`);
    } catch (error) {
      this.logger.error(`Failed to send event to DLQ: ${event.event_id}`, error);
    }
  }

  /**
   * Get outbox statistics for monitoring
   */
  async getOutboxStats(): Promise<{
    pending: number;
    sent: number;
    failed: number;
    total: number;
  }> {
    const query = `
      SELECT 
        status,
        COUNT(*) as count
      FROM ${TABLE_NAMES.OUTBOX_EVENTS}
      WHERE service_name = $1
      GROUP BY status
    `;

    const result = await this.databaseAdapter.query(query, [this.serviceName]);

    const stats = {
      pending: 0,
      sent: 0,
      failed: 0,
      total: 0
    };

    if (result.success && result.data) {
      for (const row of result.data) {
        const status = row.status as EventStatus;
        const count = parseInt(row.count);
        
        switch (status) {
          case EventStatus.PENDING:
            stats.pending = count;
            break;
          case EventStatus.SENT:
            stats.sent = count;
            break;
          case EventStatus.FAILED:
            stats.failed = count;
            break;
        }
        
        stats.total += count;
      }
    }

    return stats;
  }

  /**
   * Clean up old events from outbox (for events marked as sent)
   * This should be called periodically to prevent outbox table from growing too large
   */
  async cleanupSentEvents(olderThanDays: number = 7): Promise<number> {
    const query = `
      DELETE FROM ${TABLE_NAMES.OUTBOX_EVENTS}
      WHERE status = $1 AND service_name = $2 
        AND created_at < NOW() - INTERVAL '${olderThanDays} days'
    `;

    const result = await this.databaseAdapter.query(query, [EventStatus.SENT, this.serviceName]);

    const deletedCount = result.affectedRows || 0;
    
    if (result.success) {
      this.logger.log(`Cleaned up ${deletedCount} sent events older than ${olderThanDays} days`);
    } else {
      this.logger.error('Failed to cleanup sent events', result.error);
    }

    return deletedCount;
  }

  /**
   * Validate event data before publishing
   */
  private validateEventData(eventData: OutboxEventData): string[] {
    const errors: string[] = [];

    if (!eventData.eventId || eventData.eventId.trim() === '') {
      errors.push('Event ID is required');
    }

    if (!eventData.eventType || eventData.eventType.trim() === '') {
      errors.push('Event type is required');
    }

    if (!eventData.payload || typeof eventData.payload !== 'object') {
      errors.push('Event payload must be a valid object');
    }

    // Validate event ID format (should be UUID)
    const uuidRegex = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
    if (eventData.eventId && !uuidRegex.test(eventData.eventId)) {
      errors.push('Event ID must be a valid UUID');
    }

    return errors;
  }

  /**
   * Publish event with validation
   */
  async publishEventWithValidation(eventData: OutboxEventData): Promise<OutboxPublishResult> {
    // Validate event data
    const validationErrors = this.validateEventData(eventData);
    if (validationErrors.length > 0) {
      return {
        success: false,
        error: `Validation failed: ${validationErrors.join(', ')}`,
        retryable: false
      };
    }

    return await this.publishEvent(eventData);
  }

  /**
   * Generate a new event ID
   */
  generateEventId(): string {
    return uuidv4();
  }
}
