import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';
import { ResolvedEventStreamConfig } from '../interfaces';
import { DatabaseAdapter } from '../adapters/db.adapter';
import { RedisAdapter } from '../adapters/redis.adapter';
import { ReplayManager } from '../managers/replay.manager';
import { ReplayStrategy, ReplayMode } from '../replay/interfaces';

/**
 * Point-in-time recovery options
 */
export interface PointInTimeRecoveryOptions {
  /**
   * Target timestamp to recover to
   */
  targetTimestamp: Date;

  /**
   * Whether to include events exactly at target timestamp
   * @default true
   */
  inclusive?: boolean;

  /**
   * Whether to validate events during recovery
   * @default true
   */
  validate?: boolean;

  /**
   * Whether to skip failed events
   * @default false
   */
  skipFailures?: boolean;

  /**
   * Batch size for recovery operations
   * @default 100
   */
  batchSize?: number;

  /**
   * Custom event filter function
   */
  filter?: (event: any) => boolean | Promise<boolean>;

  /**
   * Custom event transform function
   */
  transform?: (event: any) => any | Promise<any>;
}

/**
 * Point-in-time recovery result
 */
export interface PointInTimeRecoveryResult {
  /**
   * Recovery ID
   */
  recoveryId: string;

  /**
   * Target timestamp
   */
  targetTimestamp: Date;

  /**
   * Number of events recovered
   */
  eventsRecovered: number;

  /**
   * Number of events failed
   */
  eventsFailed: number;

  /**
   * Number of events skipped
   */
  eventsSkipped: number;

  /**
   * Start time of recovery
   */
  startTime: Date;

  /**
   * End time of recovery
   */
  endTime: Date;

  /**
   * Error message (if failed)
   */
  error?: string;
}

/**
 * Point-in-time recovery error
 */
export class PointInTimeRecoveryError extends Error {
  constructor(
    message: string,
    public readonly recoveryId: string,
    public readonly error?: Error
  ) {
    super(message);
    this.name = 'PointInTimeRecoveryError';
  }

  override readonly name: string;
}

/**
 * Point-in-time recovery service
 */
@Injectable()
export class PointInTimeRecovery {
  private readonly logger = new Logger(PointInTimeRecovery.name);
  private readonly config: ResolvedEventStreamConfig['replay'];
  private readonly serviceName: string;

  constructor(
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig,
    private readonly dbAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
    private readonly replayManager: ReplayManager,
  ) {
    this.config = eventConfig.replay;
    this.serviceName = eventConfig.serviceName;
  }

  /**
   * Initialize point-in-time recovery
   */
  async initialize(): Promise<void> {
    if (!this.config.enabled) {
      return;
    }

    this.logger.log('Initializing point-in-time recovery...');

    try {
      // Create recovery history table if it doesn't exist
      await this.dbAdapter.query(`
        CREATE TABLE IF NOT EXISTS recovery_history (
          recovery_id VARCHAR(36) PRIMARY KEY,
          service_name VARCHAR(255) NOT NULL,
          target_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
          events_recovered INTEGER NOT NULL DEFAULT 0,
          events_failed INTEGER NOT NULL DEFAULT 0,
          events_skipped INTEGER NOT NULL DEFAULT 0,
          start_time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          end_time TIMESTAMP WITH TIME ZONE,
          error TEXT,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_recovery_history_service_name ON recovery_history(service_name);
        CREATE INDEX IF NOT EXISTS idx_recovery_history_created_at ON recovery_history(created_at);
      `);

      this.logger.log('Point-in-time recovery initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize point-in-time recovery:', error);
      throw error;
    }
  }

  /**
   * Recover to a point in time
   */
  async recover(options: PointInTimeRecoveryOptions): Promise<PointInTimeRecoveryResult> {
    if (!this.config.enabled) {
      throw new PointInTimeRecoveryError('Point-in-time recovery is disabled', 'N/A');
    }

    const { targetTimestamp, inclusive = true, validate = true, skipFailures = false, batchSize = 100 } = options;

    // Start replay
    const replayResult = await this.replayManager.startReplay({
      strategy: ReplayStrategy.BY_TIME_RANGE,
      mode: ReplayMode.FAST,
      startTime: new Date(0), // From beginning
      endTime: inclusive ? targetTimestamp : new Date(targetTimestamp.getTime() - 1),
      validate,
      skipFailures,
      batchSize,
      filter: options.filter,
      transform: options.transform,
    });

    // Create recovery history entry
    const result = await this.dbAdapter.query(`
      INSERT INTO recovery_history (
        recovery_id,
        service_name,
        target_timestamp,
        start_time
      ) VALUES ($1, $2, $3, $4)
      RETURNING *;
    `, [
      replayResult.replayId,
      this.serviceName,
      targetTimestamp,
      replayResult.progress.startTime,
    ]);

    if (!result.success || !result.data || !result.data[0]) {
      throw new PointInTimeRecoveryError(
        'Failed to create recovery history entry',
        replayResult.replayId
      );
    }

    // Monitor replay progress
    let recoveryResult: PointInTimeRecoveryResult;
    try {
      while (true) {
        const progress = this.replayManager.getReplayProgress(replayResult.replayId);
        if (!progress) {
          throw new PointInTimeRecoveryError(
            'Replay progress not found',
            replayResult.replayId
          );
        }

        // Update recovery history
        await this.dbAdapter.query(`
          UPDATE recovery_history
          SET events_recovered = $1,
              events_failed = $2,
              events_skipped = $3,
              updated_at = CURRENT_TIMESTAMP
          WHERE recovery_id = $4;
        `, [
          progress.successCount,
          progress.failureCount,
          progress.skippedCount,
          replayResult.replayId,
        ]);

        if (progress.status === 'COMPLETED') {
          recoveryResult = {
            recoveryId: replayResult.replayId,
            targetTimestamp,
            eventsRecovered: progress.successCount,
            eventsFailed: progress.failureCount,
            eventsSkipped: progress.skippedCount,
            startTime: progress.startTime,
            endTime: progress.endTime!,
          };
          break;
        } else if (progress.status === 'FAILED') {
          throw new PointInTimeRecoveryError(
            `Recovery failed: ${progress.error}`,
            replayResult.replayId
          );
        }

        await new Promise(resolve => setTimeout(resolve, 1000));
      }

      // Update recovery history
      await this.dbAdapter.query(`
        UPDATE recovery_history
        SET end_time = $1,
            updated_at = CURRENT_TIMESTAMP
        WHERE recovery_id = $2;
      `, [recoveryResult.endTime, replayResult.replayId]);

      return recoveryResult;

    } catch (error) {
      // Update recovery history with error
      await this.dbAdapter.query(`
        UPDATE recovery_history
        SET error = $1,
            end_time = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP
        WHERE recovery_id = $2;
      `, [
        error instanceof Error ? error.message : String(error),
        replayResult.replayId,
      ]);

      throw error;
    }
  }

  /**
   * Get recovery history
   */
  async getRecoveryHistory(recoveryId: string): Promise<PointInTimeRecoveryResult | null> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM recovery_history
      WHERE recovery_id = $1;
    `, [recoveryId]);

    if (!result.success || !result.data || !result.data[0]) {
      return null;
    }

    const row = result.data[0];
    return {
      recoveryId: row.recovery_id,
      targetTimestamp: new Date(row.target_timestamp),
      eventsRecovered: row.events_recovered,
      eventsFailed: row.events_failed,
      eventsSkipped: row.events_skipped,
      startTime: new Date(row.start_time),
      endTime: row.end_time ? new Date(row.end_time) : new Date(),
      error: row.error,
    };
  }

  /**
   * Get all recovery histories
   */
  async getAllRecoveryHistories(limit: number = 100): Promise<PointInTimeRecoveryResult[]> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM recovery_history
      WHERE service_name = $1
      ORDER BY created_at DESC
      LIMIT $2;
    `, [this.serviceName, limit]);

    if (!result.success || !result.data) {
      return [];
    }

    return result.data.map(row => ({
      recoveryId: row.recovery_id,
      targetTimestamp: new Date(row.target_timestamp),
      eventsRecovered: row.events_recovered,
      eventsFailed: row.events_failed,
      eventsSkipped: row.events_skipped,
      startTime: new Date(row.start_time),
      endTime: row.end_time ? new Date(row.end_time) : new Date(),
      error: row.error,
    }));
  }

  /**
   * Clean up old recovery histories
   */
  async cleanupRecoveryHistories(olderThanDays: number = 30): Promise<number> {
    const result = await this.dbAdapter.query(`
      DELETE FROM recovery_history
      WHERE service_name = $1
      AND created_at < NOW() - INTERVAL '${olderThanDays} days'
      RETURNING recovery_id;
    `, [this.serviceName]);

    if (!result.success || !result.data) {
      return 0;
    }

    return result.data.length;
  }
}
