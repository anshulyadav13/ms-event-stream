import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';
import { ResolvedEventStreamConfig } from '../interfaces';
import { DatabaseAdapter } from '../adapters/db.adapter';
import { RedisAdapter } from '../adapters/redis.adapter';
import { v4 as uuidv4 } from 'uuid';
import {
  ReplayStrategy,
  ReplayMode,
  ReplayStatus,
  ReplayOptions,
  ReplayEvent,
  ReplayProgress,
  ReplayHistory,
  ReplayValidationResult,
  ReplayError,
  ReplayValidationError,
  ReplayNotFoundError,
  ReplayOperationNotAllowedError,
} from '../replay/interfaces';

/**
 * Manager for event replay and recovery operations
 */
@Injectable()
export class ReplayManager {
  private readonly logger = new Logger(ReplayManager.name);
  private readonly config: ResolvedEventStreamConfig['replay'];
  private readonly serviceName: string;
  private readonly activeReplays = new Map<string, ReplayProgress>();
  private readonly replayHistory = new Map<string, ReplayHistory>();

  constructor(
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig,
    private readonly dbAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
  ) {
    this.config = eventConfig.replay;
    this.serviceName = eventConfig.serviceName;
  }

  /**
   * Initialize replay manager
   */
  async initialize(): Promise<void> {
    if (!this.config.enabled) {
      return;
    }

    this.logger.log('Initializing replay manager...');

    try {
      // Create replay history table if it doesn't exist
      await this.dbAdapter.query(`
        CREATE TABLE IF NOT EXISTS replay_history (
          replay_id VARCHAR(36) PRIMARY KEY,
          service_name VARCHAR(255) NOT NULL,
          options JSONB NOT NULL,
          progress JSONB NOT NULL,
          initiated_by VARCHAR(255),
          description TEXT,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_replay_history_service_name ON replay_history(service_name);
        CREATE INDEX IF NOT EXISTS idx_replay_history_created_at ON replay_history(created_at);
      `);

      // Load active replays from history
      await this.loadActiveReplays();

      this.logger.log('Replay manager initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize replay manager:', error);
      throw error;
    }
  }

  /**
   * Load active replays from history
   */
  private async loadActiveReplays(): Promise<void> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM replay_history
      WHERE service_name = $1
      AND progress->>'status' IN ($2, $3)
      ORDER BY created_at DESC;
    `, [this.serviceName, ReplayStatus.PENDING, ReplayStatus.IN_PROGRESS]);

    if (!result.success || !result.data) {
      return;
    }

    for (const row of result.data) {
      const replayId = row.replay_id;
      const options = row.options;
      const progress = row.progress;
      const initiatedBy = row.initiated_by;
      const description = row.description;

      this.activeReplays.set(replayId, {
        ...progress,
        status: progress.status === ReplayStatus.IN_PROGRESS ? ReplayStatus.PAUSED : progress.status,
      });

      this.replayHistory.set(replayId, {
        replayId,
        options,
        progress,
        initiatedBy,
        description,
      });
    }
  }

  /**
   * Start event replay
   */
  async startReplay(options: ReplayOptions): Promise<ReplayHistory> {
    if (!this.config.enabled) {
      throw new ReplayOperationNotAllowedError('Event replay is disabled');
    }

    // Validate replay options
    const validationResult = await this.validateReplayOptions(options);
    if (!validationResult.isValid) {
      throw new ReplayValidationError(
        'Invalid replay options',
        validationResult.errors || [],
        validationResult.warnings
      );
    }

    // Generate replay ID
    const replayId = uuidv4();

    // Initialize progress
    const progress: ReplayProgress = {
      totalEvents: 0,
      successCount: 0,
      failureCount: 0,
      skippedCount: 0,
      progressPercent: 0,
      status: ReplayStatus.PENDING,
      startTime: new Date(),
    };

    // Create replay history entry
    const history: ReplayHistory = {
      replayId,
      options,
      progress,
    };

    // Save to database
    await this.dbAdapter.query(`
      INSERT INTO replay_history (
        replay_id,
        service_name,
        options,
        progress,
        initiated_by,
        description
      ) VALUES ($1, $2, $3, $4, $5, $6);
    `, [
      replayId,
      this.serviceName,
      options,
      progress,
      history.initiatedBy,
      history.description,
    ]);

    // Add to active replays and history
    this.activeReplays.set(replayId, progress);
    this.replayHistory.set(replayId, history);

    // Start replay in background
    this.processReplay(replayId).catch(error => {
      this.logger.error(`Failed to process replay ${replayId}:`, error);
    });

    return history;
  }

  /**
   * Process event replay
   */
  private async processReplay(replayId: string): Promise<void> {
    const history = this.replayHistory.get(replayId);
    if (!history) {
      throw new ReplayNotFoundError('Replay', replayId);
    }

    const { options } = history;
    let progress = this.activeReplays.get(replayId)!;

    try {
      // Update status to in progress
      progress = await this.updateReplayProgress(replayId, {
        ...progress,
        status: ReplayStatus.IN_PROGRESS,
      });

      // Get events to replay based on strategy
      const events = await this.getEventsToReplay(options);
      progress = await this.updateReplayProgress(replayId, {
        ...progress,
        totalEvents: events.length,
      });

      // Process events in batches
      const batchSize = options.batchSize || this.config.batchSize;
      for (let i = 0; i < events.length; i += batchSize) {
        const batch = events.slice(i, i + batchSize);
        await this.processBatch(replayId, batch, options);

        // Update progress
        progress = await this.updateReplayProgress(replayId, {
          ...progress,
          progressPercent: Math.round((i + batch.length) / events.length * 100),
          eventsPerSecond: this.calculateEventsPerSecond(progress),
          estimatedTimeRemainingMs: this.calculateEstimatedTimeRemaining(progress, events.length),
        });

        // Add delay if in real-time mode
        if (options.mode === ReplayMode.REAL_TIME) {
          await this.addReplayDelay(batch);
        } else if (options.mode === ReplayMode.CUSTOM_DELAY && options.delayMs) {
          await new Promise(resolve => setTimeout(resolve, options.delayMs));
        }
      }

      // Update status to completed
      progress = await this.updateReplayProgress(replayId, {
        ...progress,
        status: ReplayStatus.COMPLETED,
        endTime: new Date(),
        progressPercent: 100,
      });

    } catch (error) {
      // Update status to failed
      progress = await this.updateReplayProgress(replayId, {
        ...progress,
        status: ReplayStatus.FAILED,
        endTime: new Date(),
        error: error instanceof Error ? error.message : String(error),
      });

      throw error;
    }
  }

  /**
   * Process a batch of events
   */
  private async processBatch(
    replayId: string,
    events: ReplayEvent[],
    options: ReplayOptions
  ): Promise<void> {
    const progress = this.activeReplays.get(replayId)!;
    const results = await Promise.allSettled(
      events.map(event => this.processEvent(event, options))
    );

    let successCount = progress.successCount;
    let failureCount = progress.failureCount;
    let skippedCount = progress.skippedCount;

    for (const result of results) {
      if (result.status === 'fulfilled') {
        successCount++;
      } else {
        if (options.skipFailures) {
          skippedCount++;
        } else {
          failureCount++;
        }
      }
    }

    await this.updateReplayProgress(replayId, {
      ...progress,
      successCount,
      failureCount,
      skippedCount,
    });
  }

  /**
   * Process a single event
   */
  private async processEvent(event: ReplayEvent, options: ReplayOptions): Promise<void> {
    try {
      // Apply filter if provided
      if (options.filter) {
        const shouldProcess = await options.filter(event);
        if (!shouldProcess) {
          return;
        }
      }

      // Apply transform if provided
      if (options.transform) {
        event = await options.transform(event);
      }

      // Validate event if enabled
      if (options.validate !== false && this.config.enableValidation) {
        // TODO: Add event validation
      }

      // TODO: Add event processing logic

    } catch (error) {
      throw new ReplayError(
        `Failed to process event ${event.eventId}: ${error instanceof Error ? error.message : String(error)}`,
        event.eventId,
        true,
        error instanceof Error ? error : undefined
      );
    }
  }

  /**
   * Get events to replay based on strategy
   */
  private async getEventsToReplay(options: ReplayOptions): Promise<ReplayEvent[]> {
    let query = `
      SELECT
        event_id,
        event_type,
        payload,
        created_at as timestamp,
        correlation_id,
        trace_id,
        version,
        sequence
      FROM outbox_events
      WHERE service_name = $1
    `;
    const params: any[] = [this.serviceName];
    let paramIndex = 2;

    switch (options.strategy) {
      case ReplayStrategy.BY_EVENT_TYPE:
        if (!options.eventTypes?.length) {
          throw new ReplayValidationError(
            'Event types must be provided for BY_EVENT_TYPE strategy',
            ['eventTypes is required']
          );
        }
        query += ` AND event_type = ANY($${paramIndex})`;
        params.push(options.eventTypes);
        paramIndex++;
        break;

      case ReplayStrategy.BY_TIME_RANGE:
        if (!options.startTime || !options.endTime) {
          throw new ReplayValidationError(
            'Start and end time must be provided for BY_TIME_RANGE strategy',
            ['startTime and endTime are required']
          );
        }
        query += ` AND created_at BETWEEN $${paramIndex} AND $${paramIndex + 1}`;
        params.push(options.startTime, options.endTime);
        paramIndex += 2;
        break;

      case ReplayStrategy.BY_CORRELATION_ID:
        if (!options.correlationIds?.length) {
          throw new ReplayValidationError(
            'Correlation IDs must be provided for BY_CORRELATION_ID strategy',
            ['correlationIds is required']
          );
        }
        query += ` AND correlation_id = ANY($${paramIndex})`;
        params.push(options.correlationIds);
        paramIndex++;
        break;

      case ReplayStrategy.BY_SEQUENCE:
        if (options.startSequence === undefined || options.endSequence === undefined) {
          throw new ReplayValidationError(
            'Start and end sequence must be provided for BY_SEQUENCE strategy',
            ['startSequence and endSequence are required']
          );
        }
        query += ` AND sequence BETWEEN $${paramIndex} AND $${paramIndex + 1}`;
        params.push(options.startSequence, options.endSequence);
        paramIndex += 2;
        break;
    }

    query += ' ORDER BY created_at ASC';

    if (options.limit) {
      query += ` LIMIT $${paramIndex}`;
      params.push(options.limit);
    }

    const result = await this.dbAdapter.query(query, params);
    if (!result.success || !result.data) {
      return [];
    }

    return result.data.map(row => ({
      eventId: row.event_id,
      eventType: row.event_type,
      payload: row.payload,
      metadata: {
        timestamp: new Date(row.timestamp),
        correlationId: row.correlation_id,
        traceId: row.trace_id,
        version: row.version,
        sequence: row.sequence,
        source: this.serviceName,
      },
    }));
  }

  /**
   * Update replay progress
   */
  private async updateReplayProgress(
    replayId: string,
    progress: ReplayProgress
  ): Promise<ReplayProgress> {
    // Update in memory
    this.activeReplays.set(replayId, progress);

    // Update history
    const history = this.replayHistory.get(replayId);
    if (history) {
      history.progress = progress;
      this.replayHistory.set(replayId, history);
    }

    // Update in database
    await this.dbAdapter.query(`
      UPDATE replay_history
      SET progress = $1,
          updated_at = CURRENT_TIMESTAMP
      WHERE replay_id = $2;
    `, [progress, replayId]);

    return progress;
  }

  /**
   * Add delay between events for real-time replay
   */
  private async addReplayDelay(events: ReplayEvent[]): Promise<void> {
    if (events.length < 2) {
      return;
    }

    const firstTimestamp = events[0].metadata.timestamp.getTime();
    const lastTimestamp = events[events.length - 1].metadata.timestamp.getTime();
    const timeDiff = lastTimestamp - firstTimestamp;

    if (timeDiff > 0) {
      await new Promise(resolve => setTimeout(resolve, timeDiff));
    }
  }

  /**
   * Calculate events per second
   */
  private calculateEventsPerSecond(progress: ReplayProgress): number {
    const totalProcessed = progress.successCount + progress.failureCount + progress.skippedCount;
    const elapsedSeconds = (Date.now() - progress.startTime.getTime()) / 1000;
    return Math.round(totalProcessed / elapsedSeconds);
  }

  /**
   * Calculate estimated time remaining
   */
  private calculateEstimatedTimeRemaining(progress: ReplayProgress, totalEvents: number): number {
    const totalProcessed = progress.successCount + progress.failureCount + progress.skippedCount;
    const remainingEvents = totalEvents - totalProcessed;
    const eventsPerSecond = this.calculateEventsPerSecond(progress);
    return Math.round(remainingEvents / eventsPerSecond * 1000);
  }

  /**
   * Validate replay options
   */
  private async validateReplayOptions(options: ReplayOptions): Promise<ReplayValidationResult> {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Check strategy-specific requirements
    switch (options.strategy) {
      case ReplayStrategy.BY_EVENT_TYPE:
        if (!options.eventTypes?.length) {
          errors.push('Event types must be provided for BY_EVENT_TYPE strategy');
        }
        break;

      case ReplayStrategy.BY_TIME_RANGE:
        if (!options.startTime || !options.endTime) {
          errors.push('Start and end time must be provided for BY_TIME_RANGE strategy');
        } else if (options.startTime > options.endTime) {
          errors.push('Start time must be before end time');
        }
        break;

      case ReplayStrategy.BY_CORRELATION_ID:
        if (!options.correlationIds?.length) {
          errors.push('Correlation IDs must be provided for BY_CORRELATION_ID strategy');
        }
        break;

      case ReplayStrategy.BY_SEQUENCE:
        if (options.startSequence === undefined || options.endSequence === undefined) {
          errors.push('Start and end sequence must be provided for BY_SEQUENCE strategy');
        } else if (options.startSequence > options.endSequence) {
          errors.push('Start sequence must be less than end sequence');
        }
        break;
    }

    // Check mode-specific requirements
    switch (options.mode) {
      case ReplayMode.CUSTOM_DELAY:
        if (!options.delayMs) {
          errors.push('Delay must be provided for CUSTOM_DELAY mode');
        } else if (options.delayMs < 0) {
          errors.push('Delay must be positive');
        }
        break;
    }

    // Check batch size
    if (options.batchSize !== undefined) {
      if (options.batchSize <= 0) {
        errors.push('Batch size must be positive');
      } else if (options.batchSize > 1000) {
        warnings.push('Large batch sizes may impact performance');
      }
    }

    // Check limit
    if (options.limit !== undefined) {
      if (options.limit <= 0) {
        errors.push('Limit must be positive');
      }
    }

    return {
      isValid: errors.length === 0,
      errors: errors.length > 0 ? errors : undefined,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Get replay progress
   */
  getReplayProgress(replayId: string): ReplayProgress | undefined {
    return this.activeReplays.get(replayId);
  }

  /**
   * Get replay history
   */
  getReplayHistory(replayId: string): ReplayHistory | undefined {
    return this.replayHistory.get(replayId);
  }

  /**
   * Get all active replays
   */
  getActiveReplays(): Map<string, ReplayProgress> {
    return new Map(this.activeReplays);
  }

  /**
   * Get all replay histories
   */
  async getAllReplayHistories(limit: number = 100): Promise<ReplayHistory[]> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM replay_history
      WHERE service_name = $1
      ORDER BY created_at DESC
      LIMIT $2;
    `, [this.serviceName, limit]);

    if (!result.success || !result.data) {
      return [];
    }

    return result.data.map(row => ({
      replayId: row.replay_id,
      options: row.options,
      progress: row.progress,
      initiatedBy: row.initiated_by,
      description: row.description,
    }));
  }

  /**
   * Cancel replay
   */
  async cancelReplay(replayId: string): Promise<void> {
    const progress = this.activeReplays.get(replayId);
    if (!progress) {
      throw new ReplayNotFoundError('Replay', replayId);
    }

    if (progress.status === ReplayStatus.COMPLETED || progress.status === ReplayStatus.FAILED) {
      throw new ReplayOperationNotAllowedError('Cannot cancel completed or failed replay');
    }

    await this.updateReplayProgress(replayId, {
      ...progress,
      status: ReplayStatus.CANCELLED,
      endTime: new Date(),
    });
  }

  /**
   * Pause replay
   */
  async pauseReplay(replayId: string): Promise<void> {
    const progress = this.activeReplays.get(replayId);
    if (!progress) {
      throw new ReplayNotFoundError('Replay', replayId);
    }

    if (progress.status !== ReplayStatus.IN_PROGRESS) {
      throw new ReplayOperationNotAllowedError('Can only pause in-progress replay');
    }

    await this.updateReplayProgress(replayId, {
      ...progress,
      status: ReplayStatus.PAUSED,
    });
  }

  /**
   * Resume replay
   */
  async resumeReplay(replayId: string): Promise<void> {
    const progress = this.activeReplays.get(replayId);
    if (!progress) {
      throw new ReplayNotFoundError('Replay', replayId);
    }

    if (progress.status !== ReplayStatus.PAUSED) {
      throw new ReplayOperationNotAllowedError('Can only resume paused replay');
    }

    await this.updateReplayProgress(replayId, {
      ...progress,
      status: ReplayStatus.IN_PROGRESS,
    });

    // Resume processing
    this.processReplay(replayId).catch(error => {
      this.logger.error(`Failed to process replay ${replayId}:`, error);
    });
  }

  /**
   * Clean up old replay histories
   */
  async cleanupReplayHistories(olderThanDays: number = 30): Promise<number> {
    const result = await this.dbAdapter.query(`
      DELETE FROM replay_history
      WHERE service_name = $1
      AND created_at < NOW() - INTERVAL '${olderThanDays} days'
      RETURNING replay_id;
    `, [this.serviceName]);

    if (!result.success || !result.data) {
      return 0;
    }

    // Remove from memory
    for (const row of result.data) {
      this.activeReplays.delete(row.replay_id);
      this.replayHistory.delete(row.replay_id);
    }

    return result.data.length;
  }
}
