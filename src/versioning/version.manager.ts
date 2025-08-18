import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';
import { ResolvedEventStreamConfig } from '../interfaces';
import { DatabaseAdapter } from '../adapters/db.adapter';
import { RedisAdapter } from '../adapters/redis.adapter';
import { v4 as uuidv4 } from 'uuid';
import * as semver from 'semver';
import { VersionCompatibilityChecker } from './compatibility.checker';
import {
  EventVersion,
  EventVersionStrategy,
  EventMigrationStrategy,
  EventVersionStatus,
  EventVersionRegistrationOptions,
  EventVersionUpdateOptions,
  EventVersionCompatibilityResult,
  EventVersionMigration,
  EventVersionMigrationStep,
  EventVersionMigrationStepType,
  EventVersionValidationResult,
  EventVersionError,
  EventVersionValidationError,
  EventVersionNotFoundError,
  EventVersionAlreadyExistsError,
  EventVersionOperationNotAllowedError,
  EventVersionCompatibilityError,
  EventVersionMigrationError,
} from './interfaces';

/**
 * Manager for event versioning and migration
 */
@Injectable()
export class EventVersionManager {
  private readonly logger = new Logger(EventVersionManager.name);
  private readonly config: ResolvedEventStreamConfig['versioning'];
  private readonly serviceName: string;
  private readonly versionCache = new Map<string, Map<string, EventVersion>>();

  constructor(
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig,
    private readonly dbAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
  ) {
    this.config = eventConfig.versioning;
    this.serviceName = eventConfig.serviceName;
  }

  /**
   * Initialize version manager
   */
  async initialize(): Promise<void> {
    if (!this.config.enabled) {
      return;
    }

    this.logger.log('Initializing event version manager...');

    try {
      // Create version registry table if it doesn't exist
      await this.dbAdapter.query(`
        CREATE TABLE IF NOT EXISTS event_versions (
          event_type VARCHAR(255) NOT NULL,
          version VARCHAR(50) NOT NULL,
          schema JSONB NOT NULL,
          description TEXT,
          status VARCHAR(20) NOT NULL,
          compatibility VARCHAR(20) NOT NULL,
          migration_strategy VARCHAR(20) NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          deprecated_at TIMESTAMP WITH TIME ZONE,
          end_of_life_at TIMESTAMP WITH TIME ZONE,
          tags TEXT[],
          PRIMARY KEY (event_type, version)
        );

        CREATE INDEX IF NOT EXISTS idx_event_versions_event_type ON event_versions(event_type);
        CREATE INDEX IF NOT EXISTS idx_event_versions_status ON event_versions(status);
        CREATE INDEX IF NOT EXISTS idx_event_versions_created_at ON event_versions(created_at);
      `);

      // Load active versions into cache if caching is enabled
      if (this.config.enableVersionCache) {
        await this.loadActiveVersions();
      }

      this.logger.log('Event version manager initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize event version manager:', error);
      throw error;
    }
  }

  /**
   * Load active versions into cache
   */
  private async loadActiveVersions(): Promise<void> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM event_versions
      WHERE status = $1
      ORDER BY event_type, version;
    `, [EventVersionStatus.ACTIVE]);

    if (!result.success || !result.data) {
      return;
    }

    for (const row of result.data) {
      const version: EventVersion = {
        eventType: row.event_type,
        version: row.version,
        schema: row.schema,
        description: row.description,
        status: row.status,
        compatibility: row.compatibility,
        migrationStrategy: row.migration_strategy,
        createdAt: new Date(row.created_at),
        updatedAt: new Date(row.updated_at),
        deprecatedAt: row.deprecated_at ? new Date(row.deprecated_at) : undefined,
        endOfLifeAt: row.end_of_life_at ? new Date(row.end_of_life_at) : undefined,
        tags: row.tags,
      };

      let eventVersions = this.versionCache.get(version.eventType);
      if (!eventVersions) {
        eventVersions = new Map<string, EventVersion>();
        this.versionCache.set(version.eventType, eventVersions);
      }
      eventVersions.set(version.version, version);
    }
  }

  /**
   * Register a new event version
   */
  async registerVersion(options: EventVersionRegistrationOptions): Promise<EventVersion> {
    if (!this.config.enabled) {
      throw new EventVersionOperationNotAllowedError('Event versioning is disabled');
    }

    const { eventType, version, schema, description, compatibility = EventVersionStrategy.BACKWARD, migrationStrategy = EventMigrationStrategy.AUTOMATIC, tags } = options;

    // Validate version format
    if (!semver.valid(version)) {
      throw new EventVersionValidationError(
        'Invalid version format',
        ['Version must be a valid semver string']
      );
    }

    // Check if version already exists
    const existingVersion = await this.findVersion(eventType, version);
    if (existingVersion) {
      throw new EventVersionAlreadyExistsError(eventType, version);
    }

    // Create new version
    const newVersion: EventVersion = {
      eventType,
      version,
      schema,
      description,
      status: EventVersionStatus.ACTIVE,
      compatibility,
      migrationStrategy,
      createdAt: new Date(),
      updatedAt: new Date(),
      tags,
    };

    // Save to database
    await this.dbAdapter.query(`
      INSERT INTO event_versions (
        event_type,
        version,
        schema,
        description,
        status,
        compatibility,
        migration_strategy,
        tags
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
    `, [
      eventType,
      version,
      schema,
      description,
      newVersion.status,
      compatibility,
      migrationStrategy,
      tags,
    ]);

    // Update cache if enabled
    if (this.config.enableVersionCache) {
      let eventVersions = this.versionCache.get(eventType);
      if (!eventVersions) {
        eventVersions = new Map<string, EventVersion>();
        this.versionCache.set(eventType, eventVersions);
      }
      eventVersions.set(version, newVersion);
    }

    return newVersion;
  }

  /**
   * Update an existing event version
   */
  async updateVersion(
    eventType: string,
    version: string,
    options: EventVersionUpdateOptions
  ): Promise<EventVersion> {
    if (!this.config.enabled) {
      throw new EventVersionOperationNotAllowedError('Event versioning is disabled');
    }

    // Get existing version
    const existingVersion = await this.findVersion(eventType, version);
    if (!existingVersion) {
      throw new EventVersionNotFoundError(eventType, version);
    }

    // Check if version can be updated
    if (existingVersion.status === EventVersionStatus.END_OF_LIFE) {
      throw new EventVersionOperationNotAllowedError('Cannot update end-of-life version');
    }

    // Build update query
    const updates: string[] = [];
    const params: any[] = [eventType, version];
    let paramIndex = 3;

    if (options.schema) {
      updates.push(`schema = $${paramIndex}`);
      params.push(options.schema);
      paramIndex++;
    }

    if (options.description !== undefined) {
      updates.push(`description = $${paramIndex}`);
      params.push(options.description);
      paramIndex++;
    }

    if (options.compatibility) {
      updates.push(`compatibility = $${paramIndex}`);
      params.push(options.compatibility);
      paramIndex++;
    }

    if (options.migrationStrategy) {
      updates.push(`migration_strategy = $${paramIndex}`);
      params.push(options.migrationStrategy);
      paramIndex++;
    }

    if (options.addTags || options.removeTags) {
      const currentTags = existingVersion.tags || [];
      const newTags = [...currentTags];

      if (options.addTags) {
        for (const tag of options.addTags) {
          if (!newTags.includes(tag)) {
            newTags.push(tag);
          }
        }
      }

      if (options.removeTags) {
        for (const tag of options.removeTags) {
          const index = newTags.indexOf(tag);
          if (index !== -1) {
            newTags.splice(index, 1);
          }
        }
      }

      updates.push(`tags = $${paramIndex}`);
      params.push(newTags);
      paramIndex++;
    }

    updates.push('updated_at = CURRENT_TIMESTAMP');

    // Execute update
    const result = await this.dbAdapter.query(`
      UPDATE event_versions
      SET ${updates.join(', ')}
      WHERE event_type = $1 AND version = $2
      RETURNING *;
    `, params);

    if (!result.success || !result.data || !result.data[0]) {
      throw new EventVersionError(
        'Failed to update version',
        eventType,
        version
      );
    }

    const row = result.data[0];
    const updatedVersion: EventVersion = {
      eventType: row.event_type,
      version: row.version,
      schema: row.schema,
      description: row.description,
      status: row.status,
      compatibility: row.compatibility,
      migrationStrategy: row.migration_strategy,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      deprecatedAt: row.deprecated_at ? new Date(row.deprecated_at) : undefined,
      endOfLifeAt: row.end_of_life_at ? new Date(row.end_of_life_at) : undefined,
      tags: row.tags,
    };

    // Update cache if enabled
    if (this.config.enableVersionCache) {
      const eventVersions = this.versionCache.get(eventType);
      if (eventVersions) {
        eventVersions.set(version, updatedVersion);
      }
    }

    return updatedVersion;
  }

  /**
   * Find event version
   */
  async findVersion(eventType: string, version: string): Promise<EventVersion | null> {
    // Check cache first if enabled
    if (this.config.enableVersionCache) {
      const eventVersions = this.versionCache.get(eventType);
      if (eventVersions) {
        const cachedVersion = eventVersions.get(version);
        if (cachedVersion) {
          return cachedVersion;
        }
      }
    }

    // Query database
    const result = await this.dbAdapter.query(`
      SELECT * FROM event_versions
      WHERE event_type = $1 AND version = $2;
    `, [eventType, version]);

    if (!result.success || !result.data || !result.data[0]) {
      return null;
    }

    const row = result.data[0];
    const foundVersion: EventVersion = {
      eventType: row.event_type,
      version: row.version,
      schema: row.schema,
      description: row.description,
      status: row.status,
      compatibility: row.compatibility,
      migrationStrategy: row.migration_strategy,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      deprecatedAt: row.deprecated_at ? new Date(row.deprecated_at) : undefined,
      endOfLifeAt: row.end_of_life_at ? new Date(row.end_of_life_at) : undefined,
      tags: row.tags,
    };

    // Update cache if enabled
    if (this.config.enableVersionCache) {
      let eventVersions = this.versionCache.get(eventType);
      if (!eventVersions) {
        eventVersions = new Map<string, EventVersion>();
        this.versionCache.set(eventType, eventVersions);
      }
      eventVersions.set(version, foundVersion);
    }

    return foundVersion;
  }

  /**
   * Get all versions for an event type
   */
  async getVersions(eventType: string): Promise<EventVersion[]> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM event_versions
      WHERE event_type = $1
      ORDER BY version DESC;
    `, [eventType]);

    if (!result.success || !result.data) {
      return [];
    }

    return result.data.map(row => ({
      eventType: row.event_type,
      version: row.version,
      schema: row.schema,
      description: row.description,
      status: row.status,
      compatibility: row.compatibility,
      migrationStrategy: row.migration_strategy,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      deprecatedAt: row.deprecated_at ? new Date(row.deprecated_at) : undefined,
      endOfLifeAt: row.end_of_life_at ? new Date(row.end_of_life_at) : undefined,
      tags: row.tags,
    }));
  }

  /**
   * Get latest version for an event type
   */
  async getLatestVersion(eventType: string): Promise<EventVersion | null> {
    const result = await this.dbAdapter.query(`
      SELECT * FROM event_versions
      WHERE event_type = $1 AND status = $2
      ORDER BY version DESC
      LIMIT 1;
    `, [eventType, EventVersionStatus.ACTIVE]);

    if (!result.success || !result.data || !result.data[0]) {
      return null;
    }

    const row = result.data[0];
    return {
      eventType: row.event_type,
      version: row.version,
      schema: row.schema,
      description: row.description,
      status: row.status,
      compatibility: row.compatibility,
      migrationStrategy: row.migration_strategy,
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      deprecatedAt: row.deprecated_at ? new Date(row.deprecated_at) : undefined,
      endOfLifeAt: row.end_of_life_at ? new Date(row.end_of_life_at) : undefined,
      tags: row.tags,
    };
  }

  /**
   * Deprecate event version
   */
  async deprecateVersion(eventType: string, version: string): Promise<void> {
    if (!this.config.enabled) {
      throw new EventVersionOperationNotAllowedError('Event versioning is disabled');
    }

    // Get existing version
    const existingVersion = await this.findVersion(eventType, version);
    if (!existingVersion) {
      throw new EventVersionNotFoundError(eventType, version);
    }

    // Check if version can be deprecated
    if (existingVersion.status !== EventVersionStatus.ACTIVE) {
      throw new EventVersionOperationNotAllowedError(`Cannot deprecate version in ${existingVersion.status} status`);
    }

    // Update version status
    await this.dbAdapter.query(`
      UPDATE event_versions
      SET status = $1,
          deprecated_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE event_type = $2 AND version = $3;
    `, [EventVersionStatus.DEPRECATED, eventType, version]);

    // Update cache if enabled
    if (this.config.enableVersionCache) {
      const eventVersions = this.versionCache.get(eventType);
      if (eventVersions) {
        const cachedVersion = eventVersions.get(version);
        if (cachedVersion) {
          cachedVersion.status = EventVersionStatus.DEPRECATED;
          cachedVersion.deprecatedAt = new Date();
          cachedVersion.updatedAt = new Date();
          eventVersions.set(version, cachedVersion);
        }
      }
    }
  }

  /**
   * End of life event version
   */
  async endOfLifeVersion(eventType: string, version: string): Promise<void> {
    if (!this.config.enabled) {
      throw new EventVersionOperationNotAllowedError('Event versioning is disabled');
    }

    // Get existing version
    const existingVersion = await this.findVersion(eventType, version);
    if (!existingVersion) {
      throw new EventVersionNotFoundError(eventType, version);
    }

    // Check if version can be end of life
    if (existingVersion.status === EventVersionStatus.END_OF_LIFE) {
      throw new EventVersionOperationNotAllowedError('Version is already end of life');
    }

    // Update version status
    await this.dbAdapter.query(`
      UPDATE event_versions
      SET status = $1,
          end_of_life_at = CURRENT_TIMESTAMP,
          updated_at = CURRENT_TIMESTAMP
      WHERE event_type = $2 AND version = $3;
    `, [EventVersionStatus.END_OF_LIFE, eventType, version]);

    // Update cache if enabled
    if (this.config.enableVersionCache) {
      const eventVersions = this.versionCache.get(eventType);
      if (eventVersions) {
        const cachedVersion = eventVersions.get(version);
        if (cachedVersion) {
          cachedVersion.status = EventVersionStatus.END_OF_LIFE;
          cachedVersion.endOfLifeAt = new Date();
          cachedVersion.updatedAt = new Date();
          eventVersions.set(version, cachedVersion);
        }
      }
    }
  }

  /**
   * Check version compatibility
   */
  async checkCompatibility(
    eventType: string,
    sourceVersion: string,
    targetVersion: string
  ): Promise<EventVersionCompatibilityResult> {
    // Get source and target versions
    const source = await this.findVersion(eventType, sourceVersion);
    if (!source) {
      throw new EventVersionNotFoundError(eventType, sourceVersion);
    }

    const target = await this.findVersion(eventType, targetVersion);
    if (!target) {
      throw new EventVersionNotFoundError(eventType, targetVersion);
    }

    // Check version status
    if (source.status === EventVersionStatus.END_OF_LIFE) {
      return {
        isCompatible: false,
        errors: ['Source version has reached end of life'],
      };
    }

    if (target.status === EventVersionStatus.END_OF_LIFE) {
      return {
        isCompatible: false,
        errors: ['Target version has reached end of life'],
      };
    }

    // Check compatibility using the compatibility checker
    const result = VersionCompatibilityChecker.checkCompatibility(
      source.schema,
      target.schema,
      target.compatibility
    );

    // Update migration details
    if (result.requiredMigrations.length > 0) {
      result.requiredMigrations[0].sourceVersion = sourceVersion;
      result.requiredMigrations[0].targetVersion = targetVersion;
    }

    return {
      isCompatible: result.isCompatible,
      errors: result.errors.length > 0 ? result.errors : undefined,
      warnings: result.warnings.length > 0 ? result.warnings : undefined,
      requiredMigrations: result.requiredMigrations.length > 0 ? result.requiredMigrations : undefined,
    };
  }

  /**
   * Create migration plan between versions
   */
  async createMigrationPlan(
    eventType: string,
    sourceVersion: string,
    targetVersion: string
  ): Promise<EventVersionMigration> {
    // Get source and target versions
    const source = await this.findVersion(eventType, sourceVersion);
    if (!source) {
      throw new EventVersionNotFoundError(eventType, sourceVersion);
    }

    const target = await this.findVersion(eventType, targetVersion);
    if (!target) {
      throw new EventVersionNotFoundError(eventType, targetVersion);
    }

    // Check version status
    if (source.status === EventVersionStatus.END_OF_LIFE) {
      throw new EventVersionOperationNotAllowedError('Source version has reached end of life');
    }

    if (target.status === EventVersionStatus.END_OF_LIFE) {
      throw new EventVersionOperationNotAllowedError('Target version has reached end of life');
    }

    // TODO: Add migration plan generation logic
    // This will be implemented in the next task

    return {
      sourceVersion,
      targetVersion,
      steps: [],
      isBreaking: false,
      description: 'Migration plan generation not implemented yet',
    };
  }

  /**
   * Validate event data against version schema
   */
  async validateData(
    eventType: string,
    version: string,
    data: any
  ): Promise<EventVersionValidationResult> {
    // Get version
    const eventVersion = await this.findVersion(eventType, version);
    if (!eventVersion) {
      throw new EventVersionNotFoundError(eventType, version);
    }

    // TODO: Add schema validation logic
    // This will be implemented in the next task

    return {
      isValid: true,
      warnings: ['Validation not implemented yet'],
    };
  }

  /**
   * Clean up old versions
   */
  async cleanupOldVersions(olderThanDays: number = 90): Promise<number> {
    if (!this.config.enabled) {
      return 0;
    }

    const result = await this.dbAdapter.query(`
      DELETE FROM event_versions
      WHERE status = $1
      AND end_of_life_at < NOW() - INTERVAL '${olderThanDays} days'
      RETURNING event_type, version;
    `, [EventVersionStatus.END_OF_LIFE]);

    if (!result.success || !result.data) {
      return 0;
    }

    // Update cache if enabled
    if (this.config.enableVersionCache) {
      for (const row of result.data) {
        const eventVersions = this.versionCache.get(row.event_type);
        if (eventVersions) {
          eventVersions.delete(row.version);
        }
      }
    }

    return result.data.length;
  }
}
