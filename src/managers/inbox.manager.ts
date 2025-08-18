import { Injectable, Logger } from '@nestjs/common';
import { DatabaseAdapter, DatabaseResult, InboxEventRecord } from '../adapters/db.adapter';
import { EventStatus, TABLE_NAMES } from '../schemas/database.schema';
import { ResolvedEventStreamConfig } from '../interfaces';
import { InboxEvent, EventPayload } from '../interfaces/event.interface';
import { RetryManager } from './retry.manager';
import { DLQManager } from './dlq.manager';

/**
 * Inbox event data from Redis Stream
 */
export interface InboxEventData {
  eventId: string;
  eventType: string;
  payload: Record<string, any>;
  correlationId?: string;
  traceId?: string;
  version?: string;
  sourceService?: string;
  createdAt: string;
  redisMessageId?: string;
}

/**
 * Inbox processing result
 */
export interface InboxProcessingResult {
  success: boolean;
  eventId?: string;
  processed: boolean; // Whether the event was actually processed (false for duplicates)
  error?: string;
  retryable?: boolean;
}

/**
 * Event processing context passed to handlers
 */
export interface EventProcessingContext {
  eventId: string;
  eventType: string;
  attempt: number;
  maxAttempts: number;
  redisMessageId?: string;
}

/**
 * Inbox manager handles the inbox pattern for reliable event consumption
 * 
 * Flow:
 * 1. Receive event from Redis Stream
 * 2. Insert into inbox_events table (status = received) - PK constraint prevents duplicates
 * 3. If new event → process & mark as processed
 * 4. If duplicate → skip processing
 * 
 * This ensures no duplicate processing even with horizontal scaling
 */
@Injectable()
export class InboxManager {
  private readonly logger = new Logger(InboxManager.name);
  private readonly serviceName: string;
  private readonly maxRetries: number;

  constructor(
    private readonly config: ResolvedEventStreamConfig,
    private readonly databaseAdapter: DatabaseAdapter,
    private readonly retryManager?: RetryManager,
    private readonly dlqManager?: DLQManager,
  ) {
    this.serviceName = config.serviceName;
    this.maxRetries = config.inbox?.maxRetries || 3;
  }

  /**
   * Process an incoming event using the inbox pattern
   * This is the main entry point for event consumption
   */
  async processIncomingEvent(
    eventData: InboxEventData,
    processingHandler: (event: EventPayload) => Promise<void>
  ): Promise<InboxProcessingResult> {
    const { eventId, eventType } = eventData;

    this.logger.debug(`Processing incoming event via inbox: ${eventType} (${eventId})`);

    try {
      // Step 1: Try to insert event into inbox with received status
      const insertResult = await this.insertEventToInbox(eventData);
      
      // If insert failed due to duplicate key constraint, event already exists
      if (!insertResult.success) {
        if (this.isDuplicateKeyError(insertResult.error)) {
          this.logger.debug(`Duplicate event detected, skipping processing: ${eventType} (${eventId})`);
          return {
            success: true,
            eventId,
            processed: false // Event was not processed (duplicate)
          };
        }

        // Other database errors
        return {
          success: false,
          eventId,
          processed: false,
          error: `Failed to insert event to inbox: ${insertResult.error || 'Unknown database error'}`,
          retryable: true
        };
      }

      // Step 2: Event is new, proceed with processing
      const processingResult = await this.executeEventProcessing(eventData, processingHandler);
      
      if (!processingResult.success) {
        // Mark as failed in inbox
        await this.updateEventStatus(eventId, EventStatus.FAILED, processingResult.error);
        
        return {
          success: false,
          eventId,
          processed: false,
          error: processingResult.error || 'Unknown processing error',
          retryable: processingResult.retryable || false
        };
      }

      // Step 3: Mark as processed in inbox
      const updateResult = await this.updateEventStatus(eventId, EventStatus.PROCESSED);
      if (!updateResult.success) {
        this.logger.warn(`Event processed but failed to update status: ${eventId}`);
        // Don't return error here as the event was actually processed
      }

      this.logger.debug(`Event processed successfully via inbox: ${eventType} (${eventId})`);

      return {
        success: true,
        eventId,
        processed: true
      };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Unexpected error during inbox processing: ${eventType} (${eventId})`, error);

      // Try to mark as failed in inbox if the event was inserted
      try {
        await this.updateEventStatus(eventId, EventStatus.FAILED, errorMessage);
      } catch (updateError) {
        this.logger.error(`Failed to update event status after error: ${eventId}`, updateError);
      }

      return {
        success: false,
        eventId,
        processed: false,
        error: errorMessage,
        retryable: true
      };
    }
  }

  /**
   * Insert event into inbox table with received status
   * Uses PK constraint on event_id for deduplication
   */
  private async insertEventToInbox(eventData: InboxEventData): Promise<DatabaseResult> {
    const { eventId, eventType, payload, correlationId, traceId, version, sourceService, createdAt } = eventData;

    // Create the full payload with metadata
    const fullPayload = {
      data: payload,
      metadata: {
        correlationId,
        traceId,
        version: version || '1.0',
        sourceService: sourceService || 'unknown',
        createdAt: createdAt || new Date().toISOString()
      }
    };

    const query = `
      INSERT INTO ${TABLE_NAMES.INBOX_EVENTS} 
      (event_id, event_type, payload, status, service_name, received_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
    `;

    const params = [
      eventId,
      eventType,
      JSON.stringify(fullPayload),
      EventStatus.RECEIVED,
      this.serviceName
    ];

    this.logger.debug(`Inserting event to inbox: ${eventType} (${eventId})`);

    const result = await this.databaseAdapter.query(query, params);
    
    if (!result.success) {
      if (this.isDuplicateKeyError(result.error)) {
        this.logger.debug(`Event already exists in inbox (duplicate): ${eventType} (${eventId})`);
      } else {
        this.logger.error(`Failed to insert event to inbox: ${eventType} (${eventId})`, result.error);
      }
    } else {
      this.logger.debug(`Event inserted to inbox successfully: ${eventType} (${eventId})`);
    }

    return result;
  }

  /**
   * Execute the actual event processing logic
   */
  private async executeEventProcessing(
    eventData: InboxEventData,
    processingHandler: (event: EventPayload) => Promise<void>
  ): Promise<{ success: boolean; error?: string; retryable?: boolean }> {
    const { eventId, eventType, payload, correlationId, traceId, version, sourceService, createdAt } = eventData;

    try {
      // Create event payload for handler
      const metadata: any = {
        eventId,
        eventType,
        createdAt: new Date(createdAt || new Date().toISOString()),
        version: version || '1.0',
        sourceService: sourceService || 'unknown'
      };

      if (correlationId) {
        metadata.correlationId = correlationId;
      }
      if (traceId) {
        metadata.traceId = traceId;
      }

      const eventPayload: EventPayload = {
        metadata,
        data: payload
      };

      this.logger.debug(`Executing processing handler for: ${eventType} (${eventId})`);

      // Call the processing handler
      await processingHandler(eventPayload);

      this.logger.debug(`Processing handler completed successfully: ${eventType} (${eventId})`);

      return { success: true };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      const isRetryable = this.isRetryableError(error);

      this.logger.error(`Processing handler failed: ${eventType} (${eventId})`, {
        error: errorMessage,
        retryable: isRetryable
      });

      return {
        success: false,
        error: errorMessage,
        retryable: isRetryable
      };
    }
  }

  /**
   * Update event status in inbox table
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
        UPDATE ${TABLE_NAMES.INBOX_EVENTS} 
        SET status = $1, error_message = $2, retry_count = retry_count + 1
        WHERE event_id = $3 AND service_name = $4
      `;
      params = [status, errorMessage, eventId, this.serviceName];
    } else if (status === EventStatus.PROCESSED) {
      query = `
        UPDATE ${TABLE_NAMES.INBOX_EVENTS} 
        SET status = $1, processed_at = NOW()
        WHERE event_id = $2 AND service_name = $3
      `;
      params = [status, eventId, this.serviceName];
    } else {
      query = `
        UPDATE ${TABLE_NAMES.INBOX_EVENTS} 
        SET status = $1
        WHERE event_id = $2 AND service_name = $3
      `;
      params = [status, eventId, this.serviceName];
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
   * Check if database error is due to duplicate key constraint
   */
  private isDuplicateKeyError(error?: string): boolean {
    if (!error) return false;

    const duplicateKeyIndicators = [
      'duplicate key',
      'unique constraint',
      'already exists',
      'violates unique constraint',
      'primary key'
    ];

    const errorLower = error.toLowerCase();
    return duplicateKeyIndicators.some(indicator => errorLower.includes(indicator));
  }

  /**
   * Determine if an error is retryable
   */
  private isRetryableError(error: unknown): boolean {
    if (!error) return false;

    const errorMessage = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

    // Network/connection errors are retryable
    const retryableErrors = [
      'connection',
      'timeout',
      'network',
      'temporarily unavailable',
      'service unavailable',
      'rate limit',
      'quota exceeded'
    ];

    // Application errors that are not retryable
    const nonRetryableErrors = [
      'validation',
      'invalid',
      'unauthorized',
      'forbidden',
      'not found',
      'bad request',
      'malformed'
    ];

    // Check for non-retryable errors first
    if (nonRetryableErrors.some(nonRetryable => errorMessage.includes(nonRetryable))) {
      return false;
    }

    // Check for retryable errors
    if (retryableErrors.some(retryable => errorMessage.includes(retryable))) {
      return true;
    }

    // Default to retryable for unknown errors
    return true;
  }

  /**
   * Get received events that haven't been processed yet
   */
  async getUnprocessedEvents(limit: number = 100): Promise<DatabaseResult<InboxEventRecord[]>> {
    const query = `
      SELECT * FROM ${TABLE_NAMES.INBOX_EVENTS}
      WHERE status = $1 AND service_name = $2
      ORDER BY received_at ASC
      LIMIT $3
    `;

    const params = [EventStatus.RECEIVED, this.serviceName, limit];

    this.logger.debug(`Fetching unprocessed events for service: ${this.serviceName}`);

    const result = await this.databaseAdapter.query<InboxEventRecord>(query, params);

    if (result.success) {
      this.logger.debug(`Found ${result.data?.length || 0} unprocessed events`);
    } else {
      this.logger.error('Failed to fetch unprocessed events', result.error);
    }

    return result;
  }

  /**
   * Get failed events that can be retried
   */
  async getRetryableFailedEvents(limit: number = 50): Promise<DatabaseResult<InboxEventRecord[]>> {
    const query = `
      SELECT * FROM ${TABLE_NAMES.INBOX_EVENTS}
      WHERE status = $1 AND service_name = $2 AND retry_count < $3
      ORDER BY received_at ASC
      LIMIT $4
    `;

    const params = [EventStatus.FAILED, this.serviceName, this.maxRetries, limit];

    this.logger.debug(`Fetching retryable failed events for service: ${this.serviceName}`);

    const result = await this.databaseAdapter.query<InboxEventRecord>(query, params);

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
  async retryFailedEvent(
    eventId: string,
    processingHandler: (event: EventPayload) => Promise<void>
  ): Promise<InboxProcessingResult> {
    this.logger.debug(`Retrying failed event: ${eventId}`);

    try {
      // First, get the event from inbox
      const query = `
        SELECT * FROM ${TABLE_NAMES.INBOX_EVENTS}
        WHERE event_id = $1 AND service_name = $2
      `;

      const result = await this.databaseAdapter.query<InboxEventRecord>(query, [eventId, this.serviceName]);

      if (!result.success || !result.data || result.data.length === 0) {
        return {
          success: false,
          processed: false,
          error: `Event not found in inbox: ${eventId}`,
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
          processed: false,
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
          processed: false,
          error: `Failed to parse event payload: ${eventId}`,
          retryable: false
        };
      }

      // Extract event data for retry
      const eventData: InboxEventData = {
        eventId: event.event_id,
        eventType: event.event_type,
        payload: parsedPayload.data,
        correlationId: parsedPayload.metadata?.correlationId,
        traceId: parsedPayload.metadata?.traceId,
        version: parsedPayload.metadata?.version,
        sourceService: parsedPayload.metadata?.sourceService,
        createdAt: parsedPayload.metadata?.createdAt || event.received_at.toISOString()
      };

      // Reset status to received
      await this.updateEventStatus(eventId, EventStatus.RECEIVED);

      // Attempt to process again
      const processingResult = await this.executeEventProcessing(eventData, processingHandler);
      
      if (!processingResult.success) {
        await this.updateEventStatus(eventId, EventStatus.FAILED, processingResult.error);
        
        return {
          success: false,
          eventId,
          processed: false,
          error: processingResult.error || 'Unknown processing error',
          retryable: processingResult.retryable || false
        };
      }

      // Mark as processed
      await this.updateEventStatus(eventId, EventStatus.PROCESSED);

      return {
        success: true,
        eventId,
        processed: true
      };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      this.logger.error(`Unexpected error during event retry: ${eventId}`, error);

      return {
        success: false,
        eventId,
        processed: false,
        error: errorMessage,
        retryable: true
      };
    }
  }

  /**
   * Get inbox statistics for monitoring
   */
  async getInboxStats(): Promise<{
    received: number;
    processed: number;
    failed: number;
    total: number;
  }> {
    const query = `
      SELECT 
        status,
        COUNT(*) as count
      FROM ${TABLE_NAMES.INBOX_EVENTS}
      WHERE service_name = $1
      GROUP BY status
    `;

    const result = await this.databaseAdapter.query(query, [this.serviceName]);

    const stats = {
      received: 0,
      processed: 0,
      failed: 0,
      total: 0
    };

    if (result.success && result.data) {
      for (const row of result.data) {
        const status = row.status as EventStatus;
        const count = parseInt(row.count);
        
        switch (status) {
          case EventStatus.RECEIVED:
            stats.received = count;
            break;
          case EventStatus.PROCESSED:
            stats.processed = count;
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
   * Check if an event exists in the inbox (for duplicate detection)
   */
  async eventExists(eventId: string): Promise<boolean> {
    const query = `
      SELECT 1 FROM ${TABLE_NAMES.INBOX_EVENTS}
      WHERE event_id = $1 AND service_name = $2
      LIMIT 1
    `;

    const result = await this.databaseAdapter.query(query, [eventId, this.serviceName]);

    return !!(result.success && result.data && result.data.length > 0);
  }

  /**
   * Get event by ID from inbox
   */
  async getEventById(eventId: string): Promise<DatabaseResult<InboxEventRecord | null>> {
    const query = `
      SELECT * FROM ${TABLE_NAMES.INBOX_EVENTS}
      WHERE event_id = $1 AND service_name = $2
    `;

    const result = await this.databaseAdapter.query<InboxEventRecord>(query, [eventId, this.serviceName]);

    if (result.success && result.data && result.data.length > 0) {
      return {
        ...result,
        data: result.data[0]
      };
    }

    return {
      success: result.success,
      data: null,
      ...(result.error && { error: result.error })
    };
  }

  /**
   * Clean up old processed events from inbox
   * This should be called periodically to prevent inbox table from growing too large
   */
  async cleanupProcessedEvents(olderThanDays: number = 7): Promise<number> {
    const query = `
      DELETE FROM ${TABLE_NAMES.INBOX_EVENTS}
      WHERE status = $1 AND service_name = $2 
        AND processed_at < NOW() - INTERVAL '${olderThanDays} days'
    `;

    const result = await this.databaseAdapter.query(query, [EventStatus.PROCESSED, this.serviceName]);

    const deletedCount = result.affectedRows || 0;
    
    if (result.success) {
      this.logger.log(`Cleaned up ${deletedCount} processed events older than ${olderThanDays} days`);
    } else {
      this.logger.error('Failed to cleanup processed events', result.error);
    }

    return deletedCount;
  }

  /**
   * Validate incoming event data
   */
  private validateEventData(eventData: InboxEventData): string[] {
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
   * Process event with validation
   */
  async processIncomingEventWithValidation(
    eventData: InboxEventData,
    processingHandler: (event: EventPayload) => Promise<void>
  ): Promise<InboxProcessingResult> {
    // Validate event data
    const validationErrors = this.validateEventData(eventData);
    if (validationErrors.length > 0) {
      return {
        success: false,
        processed: false,
        error: `Validation failed: ${validationErrors.join(', ')}`,
        retryable: false
      };
    }

    return await this.processIncomingEvent(eventData, processingHandler);
  }

  /**
   * Send failed event to DLQ
   */
  private async sendEventToDLQ(event: InboxEventRecord, errorMessage: string): Promise<void> {
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
        sourceService: parsedPayload.metadata?.sourceService || 'unknown',
        originalCreatedAt: parsedPayload.metadata?.createdAt || event.received_at.toISOString(),
        errorMessage,
        errorType,
        retryAttempts: event.retry_count || 0,
        maxRetryAttempts: this.maxRetries,
        processingContext: 'inbox',
        metadata: {
          inboxReceivedAt: event.received_at.toISOString(),
          inboxServiceName: event.service_name,
          originalProcessedAt: event.processed_at?.toISOString(),
        },
      });

      this.logger.log(`Event sent to DLQ: ${event.event_type} (${event.event_id})`);
    } catch (error) {
      this.logger.error(`Failed to send event to DLQ: ${event.event_id}`, error);
    }
  }
}
