import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';
import { RedisAdapter, StreamMessage } from '../adapters/redis.adapter';
import { InboxManager, InboxEventData } from '../managers/inbox.manager';
import { ResolvedEventStreamConfig } from '../interfaces';
import { EventPayload } from '../interfaces/event.interface';

/**
 * Consumer configuration options
 */
export interface ConsumerOptions {
  consumerName?: string;
  batchSize?: number;
  blockTimeMs?: number;
  maxRetries?: number;
  pollIntervalMs?: number;
  enableHealthChecks?: boolean;
}

/**
 * Consumer health status
 */
export interface ConsumerHealth {
  isRunning: boolean;
  messagesProcessed: number;
  lastProcessedAt?: Date | undefined;
  errors: number;
  lastErrorAt?: Date | undefined;
  lastError?: string | undefined;
  uptime: number;
}

/**
 * Consumer statistics
 */
export interface ConsumerStats {
  messagesProcessed: number;
  messagesAcknowledged: number;
  messagesRejected: number;
  processingErrors: number;
  averageProcessingTimeMs: number;
  lastProcessedAt?: Date | undefined;
  startTime: Date;
}

/**
 * Event processing result
 */
export interface EventProcessingResult {
  success: boolean;
  eventId: string;
  processingTimeMs: number;
  error?: string | undefined;
  retryable?: boolean | undefined;
}

/**
 * Redis Stream event consumer for reliable event consumption
 * 
 * Features:
 * - Consumer groups for horizontal scaling
 * - Message acknowledgment/rejection
 * - Backpressure handling
 * - Health monitoring
 * - Graceful shutdown
 */
@Injectable()
export class EventConsumer implements OnApplicationShutdown {
  private readonly logger = new Logger(EventConsumer.name);
  private readonly serviceName: string;
  private readonly consumerName: string;
  private readonly streamName: string;
  private readonly groupName: string;
  
  // Consumer configuration
  private readonly batchSize: number;
  private readonly blockTimeMs: number;
  private readonly pollIntervalMs: number;
  private readonly maxRetries: number;
  private readonly enableHealthChecks: boolean;
  
  // Consumer state
  private isRunning = false;
  private isShuttingDown = false;
  private pollTimer?: NodeJS.Timeout | undefined;
  private processingHandler?: (event: EventPayload) => Promise<void>;
  
  // Statistics and health
  private stats: ConsumerStats;
  private health: ConsumerHealth;
  private lastHealthCheck = new Date();

  constructor(
    private readonly config: ResolvedEventStreamConfig,
    private readonly redisAdapter: RedisAdapter,
    private readonly inboxManager: InboxManager,
    options: ConsumerOptions = {}
  ) {
    this.serviceName = config.serviceName;
    this.consumerName = options.consumerName || `${this.serviceName}-consumer-${uuidv4().slice(0, 8)}`;
    this.streamName = this.redisAdapter.getStreamNames().events;
    this.groupName = this.redisAdapter.getConsumerGroupName();
    
    // Configure consumer options
    this.batchSize = options.batchSize || config.consumer?.batchSize || 10;
    this.blockTimeMs = options.blockTimeMs || config.consumer?.blockTimeMs || 1000;
    this.pollIntervalMs = options.pollIntervalMs || config.consumer?.pollIntervalMs || 5000;
    this.maxRetries = options.maxRetries || config.consumer?.maxRetries || 3;
    this.enableHealthChecks = options.enableHealthChecks !== false;
    
    // Initialize stats and health
    this.stats = {
      messagesProcessed: 0,
      messagesAcknowledged: 0,
      messagesRejected: 0,
      processingErrors: 0,
      averageProcessingTimeMs: 0,
      startTime: new Date()
    };
    
    this.health = {
      isRunning: false,
      messagesProcessed: 0,
      errors: 0,
      uptime: 0
    };
    
    this.logger.log(`Consumer initialized: ${this.consumerName} for stream: ${this.streamName}`);
  }

  /**
   * Start the consumer
   */
  async start(processingHandler: (event: EventPayload) => Promise<void>): Promise<void> {
    if (this.isRunning) {
      this.logger.warn(`Consumer ${this.consumerName} is already running`);
      return;
    }

    if (this.isShuttingDown) {
      throw new Error('Cannot start consumer during shutdown');
    }

    this.processingHandler = processingHandler;
    this.isRunning = true;
    this.health.isRunning = true;
    this.stats.startTime = new Date();

    this.logger.log(`Starting consumer: ${this.consumerName}`);

    // Start polling for messages
    this.startPolling();

    this.logger.log(`Consumer started successfully: ${this.consumerName}`);
  }

  /**
   * Stop the consumer gracefully
   */
  async stop(): Promise<void> {
    if (!this.isRunning) {
      this.logger.warn(`Consumer ${this.consumerName} is not running`);
      return;
    }

    this.logger.log(`Stopping consumer: ${this.consumerName}`);
    this.isShuttingDown = true;

    // Stop polling
    if (this.pollTimer) {
      clearTimeout(this.pollTimer);
      this.pollTimer = undefined;
    }

    // Wait for current processing to complete (with timeout)
    const shutdownTimeoutMs = this.config.consumer?.shutdownTimeoutMs || 30000;
    const shutdownStart = Date.now();
    
    while (this.isRunning && (Date.now() - shutdownStart) < shutdownTimeoutMs) {
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    if (this.isRunning) {
      this.logger.warn(`Consumer shutdown timeout exceeded: ${this.consumerName}`);
    }

    this.isRunning = false;
    this.health.isRunning = false;
    this.isShuttingDown = false;

    this.logger.log(`Consumer stopped: ${this.consumerName}`);
  }

  /**
   * Start polling for messages from Redis Stream
   */
  private startPolling(): void {
    const poll = async () => {
      if (!this.isRunning || this.isShuttingDown) {
        return;
      }

      try {
        await this.pollMessages();
      } catch (error) {
        this.logger.error(`Error during message polling: ${this.consumerName}`, error);
        this.incrementErrorCount(error);
      }

      // Schedule next poll
      if (this.isRunning && !this.isShuttingDown) {
        this.pollTimer = setTimeout(poll, this.pollIntervalMs);
      }
    };

    // Start polling immediately
    poll();
  }

  /**
   * Poll for messages from Redis Stream
   */
  private async pollMessages(): Promise<void> {
    const startTime = Date.now();

    try {
      // Read messages from stream using consumer group
      const result = await this.redisAdapter.readFromStream(
        this.streamName,
        this.groupName,
        this.consumerName,
        this.batchSize,
        this.blockTimeMs
      );

      if (!result.success) {
        throw new Error(`Failed to read from stream: ${result.error}`);
      }

      const messages = result.data || [];
      
      if (messages.length === 0) {
        // No new messages, update health check
        this.updateHealthCheck();
        return;
      }

      this.logger.debug(`Received ${messages.length} messages from stream: ${this.streamName}`);

      // Process messages in batch
      await this.processMessageBatch(messages);

      this.updateHealthCheck();

    } catch (error) {
      this.logger.error(`Failed to poll messages from stream: ${this.streamName}`, error);
      this.incrementErrorCount(error);
      throw error;
    }
  }

  /**
   * Process a batch of messages
   */
  private async processMessageBatch(messages: StreamMessage[]): Promise<void> {
    const processingPromises = messages.map(message => this.processMessage(message));
    
    // Process messages concurrently but wait for all to complete
    const results = await Promise.allSettled(processingPromises);
    
    // Log any failed message processing
    results.forEach((result, index) => {
      if (result.status === 'rejected') {
        const message = messages[index];
        this.logger.error(`Failed to process message ${message.id}:`, result.reason);
      }
    });
  }

  /**
   * Process a single message
   */
  private async processMessage(message: StreamMessage): Promise<EventProcessingResult> {
    const startTime = Date.now();
    let eventId: string = 'unknown';

    try {
      // Parse event data from Redis message
      const eventData = this.parseEventFromMessage(message);
      eventId = eventData.eventId;

      this.logger.debug(`Processing message: ${message.id} for event: ${eventData.eventType} (${eventId})`);

      // Use inbox manager for deduplication and processing
      const processingResult = await this.inboxManager.processIncomingEventWithValidation(
        eventData,
        this.processingHandler!
      );

      const processingTime = Date.now() - startTime;

      if (processingResult.success) {
        // Acknowledge message in Redis
        await this.acknowledgeMessage(message.id);
        
        this.incrementStats('processed', processingTime);
        
        this.logger.debug(`Message processed successfully: ${message.id} (${eventId}) in ${processingTime}ms`);
        
        return {
          success: true,
          eventId,
          processingTimeMs: processingTime
        };
      } else {
        // Handle processing failure
        if (processingResult.retryable) {
          // For retryable errors, we don't acknowledge - let Redis retry via consumer group
          this.logger.warn(`Message processing failed (retryable): ${message.id} (${eventId}) - ${processingResult.error}`);
          this.incrementStats('rejected', processingTime);
        } else {
          // For non-retryable errors, acknowledge to prevent infinite retries
          await this.acknowledgeMessage(message.id);
          this.logger.error(`Message processing failed (non-retryable): ${message.id} (${eventId}) - ${processingResult.error}`);
          this.incrementStats('rejected', processingTime);
        }

        return {
          success: false,
          eventId,
          processingTimeMs: processingTime,
          error: processingResult.error,
          retryable: processingResult.retryable
        };
      }

    } catch (error) {
      const processingTime = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Unexpected error processing message: ${message.id} (${eventId})`, error);
      
      // For unexpected errors, acknowledge to prevent infinite retries
      try {
        await this.acknowledgeMessage(message.id);
      } catch (ackError) {
        this.logger.error(`Failed to acknowledge message after error: ${message.id}`, ackError);
      }
      
      this.incrementStats('error', processingTime);
      this.incrementErrorCount(error);

      return {
        success: false,
        eventId,
        processingTimeMs: processingTime,
        error: errorMessage,
        retryable: false
      };
    }
  }

  /**
   * Parse event data from Redis stream message
   */
  private parseEventFromMessage(message: StreamMessage): InboxEventData {
    try {
      const fields = message.message;
      
      // Extract required fields
      const eventId = fields.eventId;
      const eventType = fields.eventType;
      const payloadStr = fields.payload;
      
      if (!eventId || !eventType || !payloadStr) {
        throw new Error(`Missing required fields in message: ${message.id}`);
      }

      // Parse payload
      let payload: Record<string, any>;
      try {
        payload = JSON.parse(payloadStr);
      } catch (parseError) {
        throw new Error(`Invalid JSON payload in message: ${message.id}`);
      }

      // Extract optional metadata
      const correlationId = fields.correlationId;
      const traceId = fields.traceId;
      const version = fields.version || '1.0';
      const sourceService = fields.sourceService;
      const createdAt = fields.createdAt || new Date().toISOString();

      return {
        eventId,
        eventType,
        payload,
        correlationId,
        traceId,
        version,
        sourceService,
        createdAt,
        redisMessageId: message.id
      };

    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      throw new Error(`Failed to parse event from message ${message.id}: ${errorMessage}`);
    }
  }

  /**
   * Acknowledge message processing in Redis
   */
  private async acknowledgeMessage(messageId: string): Promise<void> {
    try {
      const result = await this.redisAdapter.acknowledgeMessage(
        this.streamName,
        this.groupName,
        messageId
      );

      if (!result.success) {
        throw new Error(`Failed to acknowledge message: ${result.error}`);
      }

      this.incrementStats('acknowledged');

    } catch (error) {
      this.logger.error(`Failed to acknowledge message: ${messageId}`, error);
      throw error;
    }
  }

  /**
   * Update statistics
   */
  private incrementStats(type: 'processed' | 'acknowledged' | 'rejected' | 'error', processingTime?: number): void {
    switch (type) {
      case 'processed':
        this.stats.messagesProcessed++;
        this.health.messagesProcessed++;
        this.health.lastProcessedAt = new Date();
        if (processingTime !== undefined) {
          this.updateAverageProcessingTime(processingTime);
        }
        break;
      case 'acknowledged':
        this.stats.messagesAcknowledged++;
        break;
      case 'rejected':
        this.stats.messagesRejected++;
        break;
      case 'error':
        this.stats.processingErrors++;
        break;
    }
  }

  /**
   * Update average processing time using exponential moving average
   */
  private updateAverageProcessingTime(newTime: number): void {
    if (this.stats.averageProcessingTimeMs === 0) {
      this.stats.averageProcessingTimeMs = newTime;
    } else {
      // Use exponential moving average with alpha = 0.1
      this.stats.averageProcessingTimeMs = 
        (0.9 * this.stats.averageProcessingTimeMs) + (0.1 * newTime);
    }
  }

  /**
   * Increment error count and update health
   */
  private incrementErrorCount(error: unknown): void {
    this.health.errors++;
    this.health.lastErrorAt = new Date();
    this.health.lastError = error instanceof Error ? error.message : String(error);
  }

  /**
   * Update health check information
   */
  private updateHealthCheck(): void {
    if (!this.enableHealthChecks) {
      return;
    }

    this.lastHealthCheck = new Date();
    this.health.uptime = Date.now() - this.stats.startTime.getTime();
  }

  /**
   * Get consumer health status
   */
  getHealth(): ConsumerHealth {
    return { ...this.health };
  }

  /**
   * Get consumer statistics
   */
  getStats(): ConsumerStats {
    return { ...this.stats };
  }

  /**
   * Get consumer configuration
   */
  getConfig(): {
    consumerName: string;
    streamName: string;
    groupName: string;
    batchSize: number;
    blockTimeMs: number;
    pollIntervalMs: number;
    maxRetries: number;
  } {
    return {
      consumerName: this.consumerName,
      streamName: this.streamName,
      groupName: this.groupName,
      batchSize: this.batchSize,
      blockTimeMs: this.blockTimeMs,
      pollIntervalMs: this.pollIntervalMs,
      maxRetries: this.maxRetries
    };
  }

  /**
   * Check if consumer is running
   */
  isConsumerRunning(): boolean {
    return this.isRunning && !this.isShuttingDown;
  }

  /**
   * Check if consumer is healthy
   */
  isHealthy(): boolean {
    if (!this.isRunning) {
      return false;
    }

    const now = Date.now();
    const timeSinceLastHealth = now - this.lastHealthCheck.getTime();
    
    // Consider unhealthy if no health update in last 30 seconds
    if (timeSinceLastHealth > 30000) {
      return false;
    }

    // Consider unhealthy if too many recent errors
    const errorRate = this.health.errors / Math.max(this.health.messagesProcessed, 1);
    if (errorRate > 0.1) { // More than 10% error rate
      return false;
    }

    return true;
  }

  /**
   * Reset statistics (useful for monitoring systems)
   */
  resetStats(): void {
    this.stats = {
      messagesProcessed: 0,
      messagesAcknowledged: 0,
      messagesRejected: 0,
      processingErrors: 0,
      averageProcessingTimeMs: 0,
      startTime: new Date()
    };

    this.health.messagesProcessed = 0;
    this.health.errors = 0;
    this.health.lastError = undefined;
    this.health.lastErrorAt = undefined;
    
    this.logger.log(`Statistics reset for consumer: ${this.consumerName}`);
  }

  /**
   * Handle application shutdown
   */
  async onApplicationShutdown(): Promise<void> {
    this.logger.log(`Shutting down consumer: ${this.consumerName}`);
    await this.stop();
  }
}
