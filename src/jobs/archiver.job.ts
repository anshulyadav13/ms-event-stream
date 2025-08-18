import { Injectable, Logger } from '@nestjs/common';
import { ArchiveManager } from '../managers/archive.manager';

/**
 * Configuration for the archiver job
 */
export interface ArchiverJobConfig {
  /**
   * Enable automatic archival job
   * @default true
   */
  enabled: boolean;

  /**
   * Cron schedule for archival job
   * @default '0 2 * * *' (daily at 2 AM)
   */
  archivalSchedule: string;

  /**
   * Cron schedule for cleanup job
   * @default '0 3 * * *' (daily at 3 AM)
   */
  cleanupSchedule: string;

  /**
   * Enable cleanup job
   * @default true
   */
  enableCleanup: boolean;

  /**
   * Maximum concurrent archival operations
   * @default 1
   */
  maxConcurrentArchivals: number;

  /**
   * Job timeout in milliseconds
   * @default 3600000 (1 hour)
   */
  jobTimeoutMs: number;

  /**
   * Enable job monitoring and metrics
   * @default true
   */
  enableMonitoring: boolean;
}

/**
 * Job execution result
 */
export interface JobExecutionResult {
  success: boolean;
  jobType: 'archival' | 'cleanup';
  startTime: Date;
  endTime: Date;
  durationMs: number;
  result?: any;
  error?: string;
}

/**
 * Job statistics
 */
export interface JobStats {
  totalExecutions: number;
  successfulExecutions: number;
  failedExecutions: number;
  averageDurationMs: number;
  lastExecution?: Date;
  lastSuccess?: Date;
  lastFailure?: Date;
  lastError?: string;
}

/**
 * Background job for handling event archival operations
 * 
 * Features:
 * - Scheduled archival of old events
 * - Automatic cleanup of old archives
 * - Job monitoring and statistics
 * - Configurable schedules and timeouts
 * - Concurrent operation management
 */
@Injectable()
export class ArchiverJob {
  private readonly logger = new Logger(ArchiverJob.name);
  private readonly config: ArchiverJobConfig;
  private isArchivalRunning = false;
  private isCleanupRunning = false;
  private archivalStats: JobStats = {
    totalExecutions: 0,
    successfulExecutions: 0,
    failedExecutions: 0,
    averageDurationMs: 0,
  };
  private cleanupStats: JobStats = {
    totalExecutions: 0,
    successfulExecutions: 0,
    failedExecutions: 0,
    averageDurationMs: 0,
  };

  constructor(
    private readonly archiveManager: ArchiveManager
  ) {
    this.config = {
      enabled: true,
      archivalSchedule: '0 2 * * *', // Daily at 2 AM
      cleanupSchedule: '0 3 * * *', // Daily at 3 AM
      enableCleanup: true,
      maxConcurrentArchivals: 1,
      jobTimeoutMs: 3600000, // 1 hour
      enableMonitoring: true,
    };
  }

  /**
   * Start scheduled jobs
   */
  startScheduledJobs(): void {
    if (!this.config.enabled) {
      this.logger.log('Scheduled jobs are disabled');
      return;
    }

    // Schedule archival job (daily at 2 AM)
    this.scheduleArchivalJob();
    
    // Schedule cleanup job (daily at 3 AM)
    if (this.config.enableCleanup) {
      this.scheduleCleanupJob();
    }

    this.logger.log('Scheduled jobs started');
  }

  /**
   * Schedule archival job
   */
  private scheduleArchivalJob(): void {
    const now = new Date();
    const nextRun = new Date(now);
    nextRun.setDate(nextRun.getDate() + 1);
    nextRun.setHours(2, 0, 0, 0);

    const delayMs = nextRun.getTime() - now.getTime();
    
    setTimeout(async () => {
      await this.executeArchivalJob();
      // Schedule next run
      this.scheduleArchivalJob();
    }, delayMs);

    this.logger.log(`Next archival job scheduled for ${nextRun.toISOString()}`);
  }

  /**
   * Schedule cleanup job
   */
  private scheduleCleanupJob(): void {
    const now = new Date();
    const nextRun = new Date(now);
    nextRun.setDate(nextRun.getDate() + 1);
    nextRun.setHours(3, 0, 0, 0);

    const delayMs = nextRun.getTime() - now.getTime();
    
    setTimeout(async () => {
      await this.executeCleanupJob();
      // Schedule next run
      this.scheduleCleanupJob();
    }, delayMs);

    this.logger.log(`Next cleanup job scheduled for ${nextRun.toISOString()}`);
  }

  /**
   * Execute archival job with monitoring
   */
  private async executeArchivalJob(): Promise<JobExecutionResult> {
    const startTime = new Date();
    this.isArchivalRunning = true;

    try {
      this.logger.log('Starting scheduled archival job');
      
      // Set job timeout
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Archival job timeout')), this.config.jobTimeoutMs);
      });

      // Execute archival with timeout
      const archivalPromise = this.archiveManager.triggerManualArchival();
      const result = await Promise.race([archivalPromise, timeoutPromise]);

      const endTime = new Date();
      const durationMs = endTime.getTime() - startTime.getTime();

      const executionResult: JobExecutionResult = {
        success: true,
        jobType: 'archival',
        startTime,
        endTime,
        durationMs,
        result
      };

      this.updateArchivalStats(executionResult);
      this.logger.log(`Archival job completed successfully in ${durationMs}ms`);

      return executionResult;

    } catch (error) {
      const endTime = new Date();
      const durationMs = endTime.getTime() - startTime.getTime();

      const executionResult: JobExecutionResult = {
        success: false,
        jobType: 'archival',
        startTime,
        endTime,
        durationMs,
        error: error instanceof Error ? error.message : 'Unknown error'
      };

      this.updateArchivalStats(executionResult);
      this.logger.error(`Archival job failed after ${durationMs}ms: ${executionResult.error}`);

      return executionResult;

    } finally {
      this.isArchivalRunning = false;
    }
  }

  /**
   * Execute cleanup job with monitoring
   */
  private async executeCleanupJob(): Promise<JobExecutionResult> {
    const startTime = new Date();
    this.isCleanupRunning = true;

    try {
      this.logger.log('Starting scheduled cleanup job');
      
      // Set job timeout
      const timeoutPromise = new Promise((_, reject) => {
        setTimeout(() => reject(new Error('Cleanup job timeout')), this.config.jobTimeoutMs);
      });

      // Execute cleanup with timeout
      const cleanupPromise = this.archiveManager.cleanupOldArchives();
      await Promise.race([cleanupPromise, timeoutPromise]);

      const endTime = new Date();
      const durationMs = endTime.getTime() - startTime.getTime();

      const executionResult: JobExecutionResult = {
        success: true,
        jobType: 'cleanup',
        startTime,
        endTime,
        durationMs
      };

      this.updateCleanupStats(executionResult);
      this.logger.log(`Cleanup job completed successfully in ${durationMs}ms`);

      return executionResult;

    } catch (error) {
      const endTime = new Date();
      const durationMs = endTime.getTime() - startTime.getTime();

      const executionResult: JobExecutionResult = {
        success: false,
        jobType: 'cleanup',
        startTime,
        endTime,
        durationMs,
        error: error instanceof Error ? error.message : 'Unknown error'
      };

      this.updateCleanupStats(executionResult);
      this.logger.error(`Cleanup job failed after ${durationMs}ms: ${executionResult.error}`);

      return executionResult;

    } finally {
      this.isCleanupRunning = false;
    }
  }

  /**
   * Update archival job statistics
   */
  private updateArchivalStats(result: JobExecutionResult): void {
    this.archivalStats.totalExecutions++;
    
    if (result.success) {
      this.archivalStats.successfulExecutions++;
      this.archivalStats.lastSuccess = result.endTime;
    } else {
      this.archivalStats.failedExecutions++;
      this.archivalStats.lastFailure = result.endTime;
      this.archivalStats.lastError = result.error || 'Unknown error';
    }

    this.archivalStats.lastExecution = result.endTime;
    
    // Update average duration
    const totalDuration = this.archivalStats.averageDurationMs * (this.archivalStats.totalExecutions - 1) + result.durationMs;
    this.archivalStats.averageDurationMs = totalDuration / this.archivalStats.totalExecutions;
  }

  /**
   * Update cleanup job statistics
   */
  private updateCleanupStats(result: JobExecutionResult): void {
    this.cleanupStats.totalExecutions++;
    
    if (result.success) {
      this.cleanupStats.successfulExecutions++;
      this.cleanupStats.lastSuccess = result.endTime;
    } else {
      this.cleanupStats.failedExecutions++;
      this.cleanupStats.lastFailure = result.endTime;
      this.cleanupStats.lastError = result.error || 'Unknown error';
    }

    this.cleanupStats.lastExecution = result.endTime;
    
    // Update average duration
    const totalDuration = this.cleanupStats.averageDurationMs * (this.cleanupStats.totalExecutions - 1) + result.durationMs;
    this.cleanupStats.averageDurationMs = totalDuration / this.cleanupStats.totalExecutions;
  }

  /**
   * Get job statistics
   */
  getJobStats(): {
    archival: JobStats;
    cleanup: JobStats;
    isArchivalRunning: boolean;
    isCleanupRunning: boolean;
  } {
    return {
      archival: this.archivalStats,
      cleanup: this.cleanupStats,
      isArchivalRunning: this.isArchivalRunning,
      isCleanupRunning: this.isCleanupRunning
    };
  }

  /**
   * Get job configuration
   */
  getJobConfig(): ArchiverJobConfig {
    return this.config;
  }

  /**
   * Manual trigger for archival job
   */
  async triggerManualArchival(): Promise<JobExecutionResult> {
    if (this.isArchivalRunning) {
      return {
        success: false,
        jobType: 'archival',
        startTime: new Date(),
        endTime: new Date(),
        durationMs: 0,
        error: 'Archival job already running'
      };
    }

    return await this.executeArchivalJob();
  }

  /**
   * Manual trigger for cleanup job
   */
  async triggerManualCleanup(): Promise<JobExecutionResult> {
    if (this.isCleanupRunning) {
      return {
        success: false,
        jobType: 'cleanup',
        startTime: new Date(),
        endTime: new Date(),
        durationMs: 0,
        error: 'Cleanup job already running'
      };
    }

    return await this.executeCleanupJob();
  }

  /**
   * Reset job statistics
   */
  resetStats(): void {
    this.archivalStats = {
      totalExecutions: 0,
      successfulExecutions: 0,
      failedExecutions: 0,
      averageDurationMs: 0,
    };
    this.cleanupStats = {
      totalExecutions: 0,
      successfulExecutions: 0,
      failedExecutions: 0,
      averageDurationMs: 0,
    };
    this.logger.log('Job statistics reset');
  }

  /**
   * Get job health status
   */
  getHealthStatus(): {
    healthy: boolean;
    archival: {
      healthy: boolean;
      lastRun: Date | undefined;
      lastSuccess: Date | undefined;
      lastFailure: Date | undefined;
      errorRate: number;
    };
    cleanup: {
      healthy: boolean;
      lastRun: Date | undefined;
      lastSuccess: Date | undefined;
      lastFailure: Date | undefined;
      errorRate: number;
    };
  } {
    const archivalErrorRate = this.archivalStats.totalExecutions > 0 
      ? this.archivalStats.failedExecutions / this.archivalStats.totalExecutions 
      : 0;

    const cleanupErrorRate = this.cleanupStats.totalExecutions > 0 
      ? this.cleanupStats.failedExecutions / this.cleanupStats.totalExecutions 
      : 0;

    return {
      healthy: archivalErrorRate < 0.1 && cleanupErrorRate < 0.1, // Less than 10% error rate
      archival: {
        healthy: archivalErrorRate < 0.1,
        lastRun: this.archivalStats.lastExecution,
        lastSuccess: this.archivalStats.lastSuccess,
        lastFailure: this.archivalStats.lastFailure,
        errorRate: archivalErrorRate
      },
      cleanup: {
        healthy: cleanupErrorRate < 0.1,
        lastRun: this.cleanupStats.lastExecution,
        lastSuccess: this.cleanupStats.lastSuccess,
        lastFailure: this.cleanupStats.lastFailure,
        errorRate: cleanupErrorRate
      }
    };
  }
}
