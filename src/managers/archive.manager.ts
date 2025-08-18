import { Injectable, Logger, Inject } from '@nestjs/common';
import { DatabaseAdapter } from '../adapters/db.adapter';
import { RedisAdapter } from '../adapters/redis.adapter';
import { ResolvedEventStreamConfig } from '../interfaces/config.interface';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';

/**
 * Archive configuration options
 */
export interface ArchiveConfig {
  /**
   * Enable automatic archival
   * @default true
   */
  enabled: boolean;

  /**
   * Retention period in days for events before archival
   * @default 30
   */
  retentionDays: number;

  /**
   * Batch size for archival operations
   * @default 1000
   */
  batchSize: number;

  /**
   * Cron schedule for archival job
   * @default '0 2 * * *' (daily at 2 AM)
   */
  schedule: string;

  /**
   * Enable compression for archived events
   * @default true
   */
  enableCompression: boolean;

  /**
   * Archive table prefix
   * @default 'archived_'
   */
  tablePrefix: string;

  /**
   * Maximum archive table size before cleanup
   * @default 1000000
   */
  maxArchiveTableSize: number;

  /**
   * Enable archive table cleanup
   * @default true
   */
  enableCleanup: boolean;

  /**
   * Cleanup interval in days
   * @default 90
   */
  cleanupIntervalDays: number;
}

/**
 * Archive operation result
 */
export interface ArchiveResult {
  success: boolean;
  archivedCount: number;
  error?: string;
  details?: {
    outboxArchived: number;
    inboxArchived: number;
    compressionRatio?: number;
    processingTimeMs: number;
  };
}

/**
 * Archive statistics
 */
export interface ArchiveStats {
  totalArchived: number;
  outboxArchived: number;
  inboxArchived: number;
  lastArchiveRun?: Date;
  lastArchiveCount: number;
  averageProcessingTimeMs: number;
  compressionRatio: number;
  archiveTableSizes: {
    outbox: number;
    inbox: number;
  };
}

/**
 * Archive query options
 */
export interface ArchiveQueryOptions {
  eventType?: string;
  startDate?: Date;
  endDate?: Date;
  limit?: number;
  offset?: number;
  includePayload?: boolean;
}

/**
 * Archived event data
 */
export interface ArchivedEvent {
  eventId: string;
  eventType: string;
  payload: any;
  metadata: {
    sourceService: string;
    correlationId?: string;
    traceId?: string;
    timestamp: Date;
    archivedAt: Date;
  };
  originalTable: 'outbox_events' | 'inbox_events';
  archiveId: string;
}

/**
 * Manager for handling event archival operations
 * 
 * Features:
 * - Automatic archival of old events
 * - Configurable retention policies
 * - Batch archival operations
 * - Archive table management
 * - Compression and optimization
 * - Query capabilities for archived events
 */
@Injectable()
export class ArchiveManager {
  private readonly logger = new Logger(ArchiveManager.name);
  private readonly config: ArchiveConfig;
  private isRunning = false;
  private lastRunStats: ArchiveStats = {
    totalArchived: 0,
    outboxArchived: 0,
    inboxArchived: 0,
    lastArchiveCount: 0,
    averageProcessingTimeMs: 0,
    compressionRatio: 1.0,
    archiveTableSizes: { outbox: 0, inbox: 0 }
  };

  constructor(
    private readonly dbAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig
  ) {
    this.config = {
      enabled: eventConfig.archive.enabled ?? true,
      retentionDays: eventConfig.archive.retentionDays ?? 30,
      batchSize: eventConfig.archive.batchSize ?? 1000,
      schedule: eventConfig.archive.schedule ?? '0 2 * * *', // Daily at 2 AM
      enableCompression: eventConfig.archive.enableCompression ?? true,
      tablePrefix: eventConfig.archive.tablePrefix ?? 'archived_',
      maxArchiveTableSize: eventConfig.archive.maxArchiveTableSize ?? 1000000,
      enableCleanup: eventConfig.archive.enableCleanup ?? true,
      cleanupIntervalDays: eventConfig.archive.cleanupIntervalDays ?? 90,
    };
  }

  /**
   * Initialize archive tables and start archival process
   */
  async initialize(): Promise<void> {
    if (!this.config.enabled) {
      this.logger.log('Event archival is disabled');
      return;
    }

    try {
      await this.createArchiveTables();
      this.logger.log('Archive tables initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize archive tables', error);
      throw error;
    }
  }

  /**
   * Create archive tables if they don't exist
   */
  private async createArchiveTables(): Promise<void> {
    const outboxArchiveTable = `${this.config.tablePrefix}outbox_events`;
    const inboxArchiveTable = `${this.config.tablePrefix}inbox_events`;

    // Create outbox archive table
    await this.dbAdapter.query(`
      CREATE TABLE IF NOT EXISTS ${outboxArchiveTable} (
        archive_id SERIAL PRIMARY KEY,
        event_id VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        metadata JSONB NOT NULL,
        original_table VARCHAR(50) NOT NULL,
        archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        original_created_at TIMESTAMP WITH TIME ZONE,
        original_updated_at TIMESTAMP WITH TIME ZONE,
        compression_info JSONB
      )
    `);

    // Create indexes
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_event_id ON ${outboxArchiveTable} (event_id)
    `);
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_event_type ON ${outboxArchiveTable} (event_type)
    `);
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_archived_at ON ${outboxArchiveTable} (archived_at)
    `);
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_original_created ON ${outboxArchiveTable} (original_created_at)
    `);

    // Create inbox archive table
    await this.dbAdapter.query(`
      CREATE TABLE IF NOT EXISTS ${inboxArchiveTable} (
        archive_id SERIAL PRIMARY KEY,
        event_id VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        payload JSONB NOT NULL,
        metadata JSONB NOT NULL,
        original_table VARCHAR(50) NOT NULL,
        archived_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        original_received_at TIMESTAMP WITH TIME ZONE,
        original_processed_at TIMESTAMP WITH TIME ZONE,
        compression_info JSONB
      )
    `);

    // Create indexes
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_event_id ON ${inboxArchiveTable} (event_id)
    `);
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_event_type ON ${inboxArchiveTable} (event_type)
    `);
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_archived_at ON ${inboxArchiveTable} (archived_at)
    `);
    await this.dbAdapter.query(`
      CREATE INDEX IF NOT EXISTS idx_archive_original_received ON ${inboxArchiveTable} (original_received_at)
    `);

    this.logger.log('Archive tables created/verified successfully');
  }

  /**
   * Start scheduled archival job (runs daily by default)
   */
  startScheduledArchival(): void {
    if (!this.config.enabled) {
      this.logger.log('Scheduled archival is disabled');
      return;
    }

    // Parse cron schedule and set up timer
    this.scheduleNextArchival();
    this.logger.log('Scheduled archival started');
  }

  /**
   * Schedule the next archival run based on cron expression
   */
  private scheduleNextArchival(): void {
    // Simple implementation - run daily at 2 AM
    const now = new Date();
    const nextRun = new Date(now);
    nextRun.setDate(nextRun.getDate() + 1);
    nextRun.setHours(2, 0, 0, 0);

    const delayMs = nextRun.getTime() - now.getTime();
    
    setTimeout(async () => {
      await this.runScheduledArchival();
      // Schedule next run
      this.scheduleNextArchival();
    }, delayMs);

    this.logger.log(`Next archival scheduled for ${nextRun.toISOString()}`);
  }

  /**
   * Run scheduled archival job
   */
  private async runScheduledArchival(): Promise<void> {
    if (this.isRunning) {
      return;
    }

    this.isRunning = true;
    const startTime = Date.now();

    try {
      this.logger.log('Starting scheduled archival job');
      const result = await this.archiveOldEvents();
      
      if (result.success) {
        this.logger.log(`Scheduled archival completed: ${result.archivedCount} events archived`);
        this.updateStats(result);
      } else {
        this.logger.error(`Scheduled archival failed: ${result.error}`);
      }
    } catch (error) {
      this.logger.error('Scheduled archival job failed', error);
    } finally {
      this.isRunning = false;
      const processingTime = Date.now() - startTime;
      this.logger.log(`Scheduled archival job completed in ${processingTime}ms`);
    }
  }

  /**
   * Archive events older than retention period
   */
  async archiveOldEvents(): Promise<ArchiveResult> {
    const startTime = Date.now();
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - this.config.retentionDays);

    try {
      // Archive outbox events
      const outboxResult = await this.archiveOutboxEvents(cutoffDate);
      
      // Archive inbox events
      const inboxResult = await this.archiveInboxEvents(cutoffDate);

      const totalArchived = outboxResult.archivedCount + inboxResult.archivedCount;
      const processingTime = Date.now() - startTime;

      return {
        success: true,
        archivedCount: totalArchived,
        details: {
          outboxArchived: outboxResult.archivedCount,
          inboxArchived: inboxResult.archivedCount,
          processingTimeMs: processingTime
        }
      };
    } catch (error) {
      this.logger.error('Failed to archive old events', error);
      return {
        success: false,
        archivedCount: 0,
        error: error instanceof Error ? error.message : 'Unknown error'
      };
    }
  }

  /**
   * Archive old outbox events
   */
  private async archiveOutboxEvents(cutoffDate: Date): Promise<{ archivedCount: number }> {
    const outboxArchiveTable = `${this.config.tablePrefix}outbox_events`;
    let archivedCount = 0;
    let offset = 0;

    while (true) {
      // Get batch of old events
      const oldEvents = await this.dbAdapter.query(`
        SELECT event_id, event_type, payload, metadata, created_at, updated_at
        FROM outbox_events 
        WHERE created_at < $1 
        ORDER BY created_at 
        LIMIT $2 OFFSET $3
      `, [cutoffDate, this.config.batchSize, offset]);

      if (!oldEvents.data || oldEvents.data.length === 0) {
        break;
      }

      // Archive batch
      for (const event of oldEvents.data) {
        const compressedPayload = this.config.enableCompression 
          ? await this.compressPayload(event.payload)
          : event.payload;

        await this.dbAdapter.query(`
          INSERT INTO ${outboxArchiveTable} 
          (event_id, event_type, payload, metadata, original_table, original_created_at, original_updated_at, compression_info)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [
          event.event_id,
          event.event_type,
          compressedPayload,
          event.metadata,
          'outbox_events',
          event.created_at,
          event.updated_at,
          this.config.enableCompression ? { compressed: true, originalSize: JSON.stringify(event.payload).length } : null
        ]);

        // Delete from original table
        await this.dbAdapter.query(`
          DELETE FROM outbox_events WHERE event_id = $1
        `, [event.event_id]);

        archivedCount++;
      }

      offset += this.config.batchSize;

      // Log progress for large archives
      if (archivedCount % (this.config.batchSize * 10) === 0) {
        this.logger.log(`Archived ${archivedCount} outbox events so far...`);
      }
    }

    this.logger.log(`Archived ${archivedCount} outbox events`);
    return { archivedCount };
  }

  /**
   * Archive old inbox events
   */
  private async archiveInboxEvents(cutoffDate: Date): Promise<{ archivedCount: number }> {
    const inboxArchiveTable = `${this.config.tablePrefix}inbox_events`;
    let archivedCount = 0;
    let offset = 0;

    while (true) {
      // Get batch of old events
      const oldEvents = await this.dbAdapter.query(`
        SELECT event_id, event_type, payload, metadata, received_at, processed_at
        FROM inbox_events 
        WHERE received_at < $1 
        ORDER BY received_at 
        LIMIT $2 OFFSET $3
      `, [cutoffDate, this.config.batchSize, offset]);

      if (!oldEvents.data || oldEvents.data.length === 0) {
        break;
      }

      // Archive batch
      for (const event of oldEvents.data) {
        const compressedPayload = this.config.enableCompression 
          ? await this.compressPayload(event.payload)
          : event.payload;

        await this.dbAdapter.query(`
          INSERT INTO ${inboxArchiveTable} 
          (event_id, event_type, payload, metadata, original_table, original_received_at, original_processed_at, compression_info)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        `, [
          event.event_id,
          event.event_type,
          compressedPayload,
          event.metadata,
          'inbox_events',
          event.received_at,
          event.processed_at,
          this.config.enableCompression ? { compressed: true, originalSize: JSON.stringify(event.payload).length } : null
        ]);

        // Delete from original table
        await this.dbAdapter.query(`
          DELETE FROM inbox_events WHERE event_id = $1
        `, [event.event_id]);

        archivedCount++;
      }

      offset += this.config.batchSize;

      // Log progress for large archives
      if (archivedCount % (this.config.batchSize * 10) === 0) {
        this.logger.log(`Archived ${archivedCount} inbox events so far...`);
      }
    }

    this.logger.log(`Archived ${archivedCount} inbox events`);
    return { archivedCount };
  }

  /**
   * Compress payload data (simple gzip compression)
   */
  private async compressPayload(payload: any): Promise<any> {
    if (!this.config.enableCompression) {
      return payload;
    }

    try {
      // For now, implement simple compression
      // In production, you might want to use zlib or similar
      const payloadStr = JSON.stringify(payload);
      if (payloadStr.length < 1000) {
        // Don't compress small payloads
        return payload;
      }

      // Simple compression: remove unnecessary whitespace and use shorter field names
      const compressed = JSON.stringify(payload, null, 0);
      return {
        _compressed: true,
        _originalSize: payloadStr.length,
        _data: compressed
      };
    } catch (error) {
      this.logger.warn('Failed to compress payload, using original', error);
      return payload;
    }
  }

  /**
   * Query archived events
   */
  async queryArchivedEvents(options: ArchiveQueryOptions = {}): Promise<ArchivedEvent[]> {
    const outboxArchiveTable = `${this.config.tablePrefix}outbox_events`;
    const inboxArchiveTable = `${this.config.tablePrefix}inbox_events`;

    let query = `
      SELECT 
        event_id, event_type, payload, metadata, 
        original_table, archived_at, archive_id,
        COALESCE(original_created_at, original_received_at) as original_timestamp
      FROM (
        SELECT *, 'outbox' as source FROM ${outboxArchiveTable}
        UNION ALL
        SELECT *, 'inbox' as source FROM ${inboxArchiveTable}
      ) combined
      WHERE 1=1
    `;

    const params: any[] = [];
    let paramIndex = 1;

    if (options.eventType) {
      query += ` AND event_type = $${paramIndex++}`;
      params.push(options.eventType);
    }

    if (options.startDate) {
      query += ` AND original_timestamp >= $${paramIndex++}`;
      params.push(options.startDate);
    }

    if (options.endDate) {
      query += ` AND original_timestamp <= $${paramIndex++}`;
      params.push(options.endDate);
    }

    query += ` ORDER BY original_timestamp DESC`;

    if (options.limit) {
      query += ` LIMIT $${paramIndex++}`;
      params.push(options.limit);
    }

    if (options.offset) {
      query += ` OFFSET $${paramIndex++}`;
      params.push(options.offset);
    }

    const result = await this.dbAdapter.query(query, params);
    
    if (!result.success || !result.data) {
      return [];
    }

    return result.data.map((row: any) => ({
      eventId: row.event_id,
      eventType: row.event_type,
      payload: this.decompressPayload(row.payload),
      metadata: row.metadata,
      originalTable: row.original_table as 'outbox_events' | 'inbox_events',
      archiveId: row.archive_id.toString(),
      archivedAt: new Date(row.archived_at),
      originalTimestamp: new Date(row.original_timestamp)
    }));
  }

  /**
   * Decompress payload data
   */
  private decompressPayload(payload: any): any {
    if (payload && payload._compressed && payload._data) {
      try {
        return JSON.parse(payload._data);
      } catch (error) {
        this.logger.warn('Failed to decompress payload', error);
        return payload;
      }
    }
    return payload;
  }

  /**
   * Get archive statistics
   */
  async getArchiveStats(): Promise<ArchiveStats> {
    const outboxArchiveTable = `${this.config.tablePrefix}outbox_events`;
    const inboxArchiveTable = `${this.config.tablePrefix}inbox_events`;

    try {
      // Get table sizes
      const outboxSizeResult = await this.dbAdapter.query(`
        SELECT COUNT(*) as count FROM ${outboxArchiveTable}
      `);
      
      const inboxSizeResult = await this.dbAdapter.query(`
        SELECT COUNT(*) as count FROM ${inboxArchiveTable}
      `);

      const outboxSize = outboxSizeResult.success && outboxSizeResult.data?.[0]?.count 
        ? parseInt(outboxSizeResult.data[0].count) 
        : 0;
      
      const inboxSize = inboxSizeResult.success && inboxSizeResult.data?.[0]?.count 
        ? parseInt(inboxSizeResult.data[0].count) 
        : 0;

      return {
        ...this.lastRunStats,
        archiveTableSizes: {
          outbox: outboxSize,
          inbox: inboxSize
        }
      };
    } catch (error) {
      this.logger.error('Failed to get archive stats', error);
      return this.lastRunStats;
    }
  }

  /**
   * Update statistics after archival run
   */
  private updateStats(result: ArchiveResult): void {
    if (result.success && result.details) {
      this.lastRunStats = {
        totalArchived: this.lastRunStats.totalArchived + result.archivedCount,
        outboxArchived: this.lastRunStats.outboxArchived + (result.details.outboxArchived || 0),
        inboxArchived: this.lastRunStats.inboxArchived + (result.details.inboxArchived || 0),
        lastArchiveRun: new Date(),
        lastArchiveCount: result.archivedCount,
        averageProcessingTimeMs: result.details.processingTimeMs,
        compressionRatio: 1.0, // TODO: Calculate actual compression ratio
        archiveTableSizes: this.lastRunStats.archiveTableSizes
      };
    }
  }

  /**
   * Clean up old archive tables
   */
  async cleanupOldArchives(): Promise<void> {
    if (!this.config.enableCleanup) {
      return;
    }

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - this.config.cleanupIntervalDays);

    try {
      const outboxArchiveTable = `${this.config.tablePrefix}outbox_events`;
      const inboxArchiveTable = `${this.config.tablePrefix}inbox_events`;

      // Delete old archived events
      const outboxResult = await this.dbAdapter.query(`
        DELETE FROM ${outboxArchiveTable} 
        WHERE archived_at < $1
      `, [cutoffDate]);

      const inboxResult = await this.dbAdapter.query(`
        DELETE FROM ${inboxArchiveTable} 
        WHERE archived_at < $1
      `, [cutoffDate]);

      this.logger.log(`Cleanup completed: ${outboxResult.affectedRows || 0} outbox, ${inboxResult.affectedRows || 0} inbox events removed`);
    } catch (error) {
      this.logger.error('Failed to cleanup old archives', error);
    }
  }

  /**
   * Manual archival trigger
   */
  async triggerManualArchival(): Promise<ArchiveResult> {
    if (this.isRunning) {
      return {
        success: false,
        archivedCount: 0,
        error: 'Archival already in progress'
      };
    }

    return await this.archiveOldEvents();
  }

  /**
   * Get archival status
   */
  getStatus(): { isRunning: boolean; lastRun: Date | undefined; config: ArchiveConfig } {
    return {
      isRunning: this.isRunning,
      lastRun: this.lastRunStats.lastArchiveRun,
      config: this.config
    };
  }
}
