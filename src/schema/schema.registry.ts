import { Injectable, Logger } from '@nestjs/common';
import { Inject } from '@nestjs/common';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';
import { ResolvedEventStreamConfig } from '../interfaces';
import { DatabaseAdapter } from '../adapters/db.adapter';
import { JSONSchema7, validate as validateSchema } from 'json-schema';
import {
  SchemaMetadata,
  SchemaVersion,
  SchemaCompatibilityLevel,
  SchemaEvolutionStrategy,
  SchemaValidationResult,
  SchemaCompatibilityResult,
  SchemaRegistrationOptions,
  SchemaUpdateOptions,
  SchemaSearchCriteria,
  SchemaCacheEntry,
  SchemaMigrationPlan,
  SchemaMigrationStep,
  SchemaValidationError,
  SchemaCompatibilityError,
  SchemaNotFoundError,
  SchemaAlreadyExistsError,
  SchemaVersionMismatchError,
  SchemaOperationNotAllowedError,
} from './interfaces';

/**
 * Schema registry for managing event schemas with versioning and compatibility checking
 */
@Injectable()
export class SchemaRegistry {
  private readonly logger = new Logger(SchemaRegistry.name);
  private readonly schemaCache = new Map<string, SchemaCacheEntry>();
  private readonly config: ResolvedEventStreamConfig['schemaRegistry'];

  constructor(
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig,
    private readonly dbAdapter: DatabaseAdapter,
  ) {
    this.config = eventConfig.schemaRegistry;
  }

  /**
   * Initialize schema registry tables and indexes
   */
  async initialize(): Promise<void> {
    this.logger.log('Initializing schema registry...');

    try {
      // Create schema_registry table if it doesn't exist
      await this.dbAdapter.query(`
        CREATE TABLE IF NOT EXISTS schema_registry (
          id SERIAL PRIMARY KEY,
          name VARCHAR(255) NOT NULL,
          namespace VARCHAR(255) NOT NULL,
          current_version INTEGER NOT NULL DEFAULT 1,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          description TEXT,
          tags TEXT[],
          UNIQUE(namespace, name)
        );

        CREATE TABLE IF NOT EXISTS schema_versions (
          id SERIAL PRIMARY KEY,
          schema_id INTEGER NOT NULL REFERENCES schema_registry(id) ON DELETE CASCADE,
          version INTEGER NOT NULL,
          schema JSONB NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
          deprecated BOOLEAN NOT NULL DEFAULT FALSE,
          deprecated_at TIMESTAMP WITH TIME ZONE,
          description TEXT,
          UNIQUE(schema_id, version)
        );

        CREATE INDEX IF NOT EXISTS idx_schema_registry_namespace_name ON schema_registry(namespace, name);
        CREATE INDEX IF NOT EXISTS idx_schema_registry_tags ON schema_registry USING GIN(tags);
        CREATE INDEX IF NOT EXISTS idx_schema_versions_schema_id ON schema_versions(schema_id);
      `);

      this.logger.log('Schema registry initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize schema registry:', error);
      throw error;
    }
  }

  /**
   * Register a new schema
   */
  async registerSchema(options: SchemaRegistrationOptions): Promise<SchemaMetadata> {
    if (!this.config.enabled) {
      throw new SchemaOperationNotAllowedError('Schema registry is disabled');
    }

    const { name, namespace, schema, version = 1, description, tags } = options;

    // Validate schema
    const validationResult = await this.validateSchema(schema);
    if (!validationResult.isValid) {
      throw new SchemaValidationError(
        'Invalid schema',
        validationResult.errors || [],
        validationResult.warnings
      );
    }

    // Check if schema already exists
    const existingSchema = await this.findSchema(namespace, name);
    if (existingSchema) {
      throw new SchemaAlreadyExistsError(namespace, name);
    }

    // Start transaction
    const client = await this.dbAdapter.getClient();
    try {
      await client.query('BEGIN');

      // Insert schema metadata
      const metadataResult = await client.query(`
        INSERT INTO schema_registry (name, namespace, current_version, description, tags)
        VALUES ($1, $2, $3, $4, $5)
        RETURNING id, created_at, updated_at;
      `, [name, namespace, version, description, tags]);

      const schemaId = metadataResult.rows[0].id;
      const createdAt = metadataResult.rows[0].created_at;
      const updatedAt = metadataResult.rows[0].updated_at;

      // Insert schema version
      await client.query(`
        INSERT INTO schema_versions (schema_id, version, schema, description)
        VALUES ($1, $2, $3, $4);
      `, [schemaId, version, schema, description]);

      await client.query('COMMIT');

      // Create schema metadata
      const metadata: SchemaMetadata = {
        name,
        namespace,
        currentVersion: version,
        versions: [{
          version,
          schema,
          createdAt: new Date(createdAt),
          description,
        }],
        createdAt: new Date(createdAt),
        updatedAt: new Date(updatedAt),
        description,
        tags,
      };

      // Update cache if enabled
      if (this.config.enableSchemaCache) {
        this.updateSchemaCache(metadata);
      }

      return metadata;

    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Update an existing schema with a new version
   */
  async updateSchema(
    namespace: string,
    name: string,
    options: SchemaUpdateOptions
  ): Promise<SchemaMetadata> {
    if (!this.config.enabled) {
      throw new SchemaOperationNotAllowedError('Schema registry is disabled');
    }

    const { schema, description, tags } = options;

    // Get existing schema
    const existingSchema = await this.findSchema(namespace, name);
    if (!existingSchema) {
      throw new SchemaNotFoundError(namespace, name);
    }

    // Validate new schema
    const validationResult = await this.validateSchema(schema);
    if (!validationResult.isValid) {
      throw new SchemaValidationError(
        'Invalid schema',
        validationResult.errors || [],
        validationResult.warnings
      );
    }

    // Check compatibility if enabled
    if (this.config.enableCompatibilityCheck) {
      const compatibilityResult = await this.checkCompatibility(
        existingSchema.versions[existingSchema.currentVersion - 1].schema,
        schema,
        options.compatibilityLevel || this.config.defaultCompatibilityLevel
      );

      if (!compatibilityResult.isCompatible) {
        throw new SchemaCompatibilityError(
          'Incompatible schema changes',
          compatibilityResult.errors || [],
          compatibilityResult.warnings
        );
      }
    }

    // Start transaction
    const client = await this.dbAdapter.getClient();
    try {
      await client.query('BEGIN');

      const newVersion = existingSchema.currentVersion + 1;

      // Insert new schema version
      await client.query(`
        INSERT INTO schema_versions (schema_id, version, schema, description)
        VALUES (
          (SELECT id FROM schema_registry WHERE namespace = $1 AND name = $2),
          $3, $4, $5
        );
      `, [namespace, name, newVersion, schema, description]);

      // Update schema metadata
      const result = await client.query(`
        UPDATE schema_registry
        SET current_version = $3,
            updated_at = CURRENT_TIMESTAMP,
            description = COALESCE($4, description),
            tags = COALESCE($5, tags)
        WHERE namespace = $1 AND name = $2
        RETURNING updated_at;
      `, [namespace, name, newVersion, description, tags]);

      await client.query('COMMIT');

      // Update schema metadata
      const metadata: SchemaMetadata = {
        ...existingSchema,
        currentVersion: newVersion,
        versions: [
          ...existingSchema.versions,
          {
            version: newVersion,
            schema,
            createdAt: new Date(),
            description,
          },
        ],
        updatedAt: new Date(result.rows[0].updated_at),
        description: description || existingSchema.description,
        tags: tags || existingSchema.tags,
      };

      // Update cache if enabled
      if (this.config.enableSchemaCache) {
        this.updateSchemaCache(metadata);
      }

      return metadata;

    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Find schema by namespace and name
   */
  async findSchema(namespace: string, name: string): Promise<SchemaMetadata | null> {
    // Check cache first
    const cacheKey = this.getCacheKey(namespace, name);
    const cachedSchema = this.schemaCache.get(cacheKey);
    if (cachedSchema && cachedSchema.expiresAt > new Date()) {
      return cachedSchema.metadata;
    }

    // Query database
    const result = await this.dbAdapter.query(`
      SELECT
        sr.name,
        sr.namespace,
        sr.current_version,
        sr.created_at,
        sr.updated_at,
        sr.description,
        sr.tags,
        json_agg(
          json_build_object(
            'version', sv.version,
            'schema', sv.schema,
            'createdAt', sv.created_at,
            'deprecated', sv.deprecated,
            'deprecatedAt', sv.deprecated_at,
            'description', sv.description
          ) ORDER BY sv.version
        ) as versions
      FROM schema_registry sr
      LEFT JOIN schema_versions sv ON sv.schema_id = sr.id
      WHERE sr.namespace = $1 AND sr.name = $2
      GROUP BY sr.id;
    `, [namespace, name]);

    if (!result.data || result.data.length === 0) {
      return null;
    }

    const row = result.data![0];
    const metadata: SchemaMetadata = {
      name: row.name,
      namespace: row.namespace,
      currentVersion: row.current_version,
      versions: row.versions.map((v: any) => ({
        version: v.version,
        schema: v.schema,
        createdAt: new Date(v.createdat),
        deprecated: v.deprecated,
        deprecatedAt: v.deprecatedat ? new Date(v.deprecatedat) : undefined,
        description: v.description,
      })),
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      description: row.description,
      tags: row.tags,
    };

    // Update cache if enabled
    if (this.config.enableSchemaCache) {
      this.updateSchemaCache(metadata);
    }

    return metadata;
  }

  /**
   * Find schema by namespace, name and version
   */
  async findSchemaVersion(
    namespace: string,
    name: string,
    version: number
  ): Promise<SchemaVersion | null> {
    const schema = await this.findSchema(namespace, name);
    if (!schema) {
      return null;
    }

    return schema.versions.find(v => v.version === version) || null;
  }

  /**
   * Search schemas by criteria
   */
  async searchSchemas(criteria: SchemaSearchCriteria): Promise<SchemaMetadata[]> {
    let query = `
      SELECT
        sr.name,
        sr.namespace,
        sr.current_version,
        sr.created_at,
        sr.updated_at,
        sr.description,
        sr.tags,
        json_agg(
          json_build_object(
            'version', sv.version,
            'schema', sv.schema,
            'createdAt', sv.created_at,
            'deprecated', sv.deprecated,
            'deprecatedAt', sv.deprecated_at,
            'description', sv.description
          ) ORDER BY sv.version
        ) as versions
      FROM schema_registry sr
      LEFT JOIN schema_versions sv ON sv.schema_id = sr.id
    `;

    const conditions: string[] = [];
    const params: any[] = [];
    let paramIndex = 1;

    if (criteria.namespace) {
      conditions.push(`sr.namespace = $${paramIndex}`);
      params.push(criteria.namespace);
      paramIndex++;
    }

    if (criteria.name) {
      conditions.push(`sr.name = $${paramIndex}`);
      params.push(criteria.name);
      paramIndex++;
    }

    if (criteria.tags) {
      conditions.push(`sr.tags && $${paramIndex}`);
      params.push(criteria.tags);
      paramIndex++;
    }

    if (criteria.deprecated !== undefined) {
      conditions.push(`sv.deprecated = $${paramIndex}`);
      params.push(criteria.deprecated);
      paramIndex++;
    }

    if (criteria.createdAfter) {
      conditions.push(`sr.created_at >= $${paramIndex}`);
      params.push(criteria.createdAfter);
      paramIndex++;
    }

    if (criteria.createdBefore) {
      conditions.push(`sr.created_at <= $${paramIndex}`);
      params.push(criteria.createdBefore);
      paramIndex++;
    }

    if (criteria.updatedAfter) {
      conditions.push(`sr.updated_at >= $${paramIndex}`);
      params.push(criteria.updatedAfter);
      paramIndex++;
    }

    if (criteria.updatedBefore) {
      conditions.push(`sr.updated_at <= $${paramIndex}`);
      params.push(criteria.updatedBefore);
      paramIndex++;
    }

    if (conditions.length > 0) {
      query += ' WHERE ' + conditions.join(' AND ');
    }

    query += ' GROUP BY sr.id';

    const result = await this.dbAdapter.query(query, params);

    return result.data!.map((row: any) => ({
      name: row.name,
      namespace: row.namespace,
      currentVersion: row.current_version,
      versions: row.versions.map((v: any) => ({
        version: v.version,
        schema: v.schema,
        createdAt: new Date(v.createdat),
        deprecated: v.deprecated,
        deprecatedAt: v.deprecatedat ? new Date(v.deprecatedat) : undefined,
        description: v.description,
      })),
      createdAt: new Date(row.created_at),
      updatedAt: new Date(row.updated_at),
      description: row.description,
      tags: row.tags,
    }));
  }

  /**
   * Deprecate a schema version
   */
  async deprecateSchemaVersion(
    namespace: string,
    name: string,
    version: number
  ): Promise<void> {
    if (!this.config.enableDeprecation) {
      throw new SchemaOperationNotAllowedError('Schema deprecation is disabled');
    }

    const schema = await this.findSchema(namespace, name);
    if (!schema) {
      throw new SchemaNotFoundError(namespace, name);
    }

    const schemaVersion = schema.versions.find(v => v.version === version);
    if (!schemaVersion) {
      throw new SchemaVersionMismatchError(namespace, name, version, schema.currentVersion);
    }

    if (version === schema.currentVersion) {
      throw new SchemaOperationNotAllowedError('Cannot deprecate current version');
    }

    await this.dbAdapter.query(`
      UPDATE schema_versions
      SET deprecated = true,
          deprecated_at = CURRENT_TIMESTAMP
      WHERE schema_id = (
        SELECT id FROM schema_registry WHERE namespace = $1 AND name = $2
      )
      AND version = $3;
    `, [namespace, name, version]);

    // Invalidate cache
    if (this.config.enableSchemaCache) {
      const cacheKey = this.getCacheKey(namespace, name);
      this.schemaCache.delete(cacheKey);
    }
  }

  /**
   * Delete a schema and all its versions
   */
  async deleteSchema(namespace: string, name: string): Promise<void> {
    if (!this.config.allowSchemaDeletion) {
      throw new SchemaOperationNotAllowedError('Schema deletion is disabled');
    }

    const schema = await this.findSchema(namespace, name);
    if (!schema) {
      throw new SchemaNotFoundError(namespace, name);
    }

    await this.dbAdapter.query(`
      DELETE FROM schema_registry
      WHERE namespace = $1 AND name = $2;
    `, [namespace, name]);

    // Invalidate cache
    if (this.config.enableSchemaCache) {
      const cacheKey = this.getCacheKey(namespace, name);
      this.schemaCache.delete(cacheKey);
    }
  }

  /**
   * Validate data against a schema
   */
  async validateData(
    namespace: string,
    name: string,
    data: any,
    version?: number
  ): Promise<SchemaValidationResult> {
    const schema = await this.findSchema(namespace, name);
    if (!schema) {
      throw new SchemaNotFoundError(namespace, name);
    }

    const schemaVersion = version
      ? schema.versions.find(v => v.version === version)
      : schema.versions[schema.currentVersion - 1];

    if (!schemaVersion) {
      throw new SchemaVersionMismatchError(namespace, name, version || schema.currentVersion, schema.currentVersion);
    }

    return this.validateSchema(data, schemaVersion.schema);
  }

  /**
   * Validate a schema
   */
  private async validateSchema(
    schema: JSONSchema7,
    parentSchema?: JSONSchema7
  ): Promise<SchemaValidationResult> {
    try {
      // Basic schema validation
      const result = validateSchema({}, schema);
      if (!result.valid) {
        return {
          isValid: false,
          errors: result.errors?.map(e => e.message),
        };
      }

      // Additional validation rules
      const warnings: string[] = [];

      // Check for required properties
      if (!schema.type) {
        warnings.push('Schema should specify a type');
      }

      if (!schema.properties && schema.type === 'object') {
        warnings.push('Object schema should define properties');
      }

      // Check for description
      if (!schema.description) {
        warnings.push('Schema should include a description');
      }

      // Check for additional properties
      if (schema.type === 'object' && schema.additionalProperties === undefined) {
        warnings.push('Consider explicitly setting additionalProperties');
      }

      return {
        isValid: true,
        warnings: warnings.length > 0 ? warnings : undefined,
      };

    } catch (error) {
      return {
        isValid: false,
        errors: [error instanceof Error ? error.message : String(error)],
      };
    }
  }

  /**
   * Check schema compatibility
   */
  private async checkCompatibility(
    oldSchema: JSONSchema7,
    newSchema: JSONSchema7,
    level: SchemaCompatibilityLevel
  ): Promise<SchemaCompatibilityResult> {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Helper function to get all property paths
    const getPropertyPaths = (schema: JSONSchema7, prefix = ''): string[] => {
      const paths: string[] = [];
      if (schema.properties) {
        for (const [key, value] of Object.entries(schema.properties)) {
          const path = prefix ? `${prefix}.${key}` : key;
          paths.push(path);
          if (typeof value === 'object' && value.properties) {
            paths.push(...getPropertyPaths(value as JSONSchema7, path));
          }
        }
      }
      return paths;
    };

    // Get property paths
    const oldPaths = getPropertyPaths(oldSchema);
    const newPaths = getPropertyPaths(newSchema);

    // Check for breaking changes
    switch (level) {
      case SchemaCompatibilityLevel.BACKWARD:
      case SchemaCompatibilityLevel.FULL:
      case SchemaCompatibilityLevel.BACKWARD_TRANSITIVE:
      case SchemaCompatibilityLevel.FULL_TRANSITIVE:
        // Check for removed required fields
        const oldRequired = oldSchema.required || [];
        const newRequired = newSchema.required || [];
        const removedRequired = oldRequired.filter(field => !newRequired.includes(field));
        if (removedRequired.length > 0) {
          errors.push(`Removed required fields: ${removedRequired.join(', ')}`);
        }

        // Check for removed properties
        const removedPaths = oldPaths.filter(path => !newPaths.includes(path));
        if (removedPaths.length > 0) {
          errors.push(`Removed properties: ${removedPaths.join(', ')}`);
        }

        // Check for type changes
        for (const path of oldPaths) {
          const oldType = this.getPropertyType(oldSchema, path);
          const newType = this.getPropertyType(newSchema, path);
          if (newType && oldType && newType !== oldType) {
            errors.push(`Type change for ${path}: ${oldType} -> ${newType}`);
          }
        }
        break;

      case SchemaCompatibilityLevel.FORWARD:
      case SchemaCompatibilityLevel.FORWARD_TRANSITIVE:
        // Check for added required fields
        const addedRequired = (newSchema.required || []).filter(field => !oldRequired.includes(field));
        if (addedRequired.length > 0) {
          errors.push(`Added required fields: ${addedRequired.join(', ')}`);
        }
        break;
    }

    // Add warnings for non-breaking changes
    const addedPaths = newPaths.filter(path => !oldPaths.includes(path));
    if (addedPaths.length > 0) {
      warnings.push(`Added properties: ${addedPaths.join(', ')}`);
    }

    return {
      isCompatible: errors.length === 0,
      level,
      errors: errors.length > 0 ? errors : undefined,
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Get property type from schema path
   */
  private getPropertyType(schema: JSONSchema7, path: string): string | undefined {
    const parts = path.split('.');
    let current: any = schema;

    for (const part of parts) {
      if (current.properties && current.properties[part]) {
        current = current.properties[part];
      } else {
        return undefined;
      }
    }

    return current.type;
  }

  /**
   * Create migration plan between schema versions
   */
  async createMigrationPlan(
    namespace: string,
    name: string,
    sourceVersion: number,
    targetVersion: number
  ): Promise<SchemaMigrationPlan> {
    const schema = await this.findSchema(namespace, name);
    if (!schema) {
      throw new SchemaNotFoundError(namespace, name);
    }

    const sourceSchema = schema.versions.find(v => v.version === sourceVersion);
    if (!sourceSchema) {
      throw new SchemaVersionMismatchError(namespace, name, sourceVersion, schema.currentVersion);
    }

    const targetSchema = schema.versions.find(v => v.version === targetVersion);
    if (!targetSchema) {
      throw new SchemaVersionMismatchError(namespace, name, targetVersion, schema.currentVersion);
    }

    const steps: SchemaMigrationStep[] = [];
    const warnings: string[] = [];

    // Compare properties
    const sourceProps = this.flattenProperties(sourceSchema.schema);
    const targetProps = this.flattenProperties(targetSchema.schema);

    // Find added fields
    for (const [path, type] of Object.entries(targetProps)) {
      if (!sourceProps[path]) {
        steps.push({
          type: 'ADD_FIELD',
          field: path,
          description: `Add field ${path} of type ${type}`,
          isBreaking: false,
        });
      }
    }

    // Find removed fields
    for (const [path, type] of Object.entries(sourceProps)) {
      if (!targetProps[path]) {
        steps.push({
          type: 'REMOVE_FIELD',
          field: path,
          description: `Remove field ${path} of type ${type}`,
          isBreaking: true,
        });
        warnings.push(`Removing field ${path} is a breaking change`);
      }
    }

    // Find modified fields
    for (const [path, sourceType] of Object.entries(sourceProps)) {
      const targetType = targetProps[path];
      if (targetType && targetType !== sourceType) {
        steps.push({
          type: 'MODIFY_FIELD',
          field: path,
          description: `Change type of ${path} from ${sourceType} to ${targetType}`,
          isBreaking: true,
        });
        warnings.push(`Changing type of ${path} from ${sourceType} to ${targetType} is a breaking change`);
      }
    }

    return {
      sourceVersion,
      targetVersion,
      steps,
      isBreaking: steps.some(step => step.isBreaking),
      warnings: warnings.length > 0 ? warnings : undefined,
    };
  }

  /**
   * Flatten schema properties into dot notation
   */
  private flattenProperties(
    schema: JSONSchema7,
    prefix = '',
    result: Record<string, string> = {}
  ): Record<string, string> {
    if (schema.properties) {
      for (const [key, value] of Object.entries(schema.properties)) {
        const path = prefix ? `${prefix}.${key}` : key;
        if (typeof value === 'object') {
          if (value.type) {
            result[path] = value.type as string;
          }
          if (value.properties) {
            this.flattenProperties(value as JSONSchema7, path, result);
          }
        }
      }
    }
    return result;
  }

  /**
   * Get cache key for schema
   */
  private getCacheKey(namespace: string, name: string): string {
    return `${namespace}:${name}`;
  }

  /**
   * Update schema cache
   */
  private updateSchemaCache(metadata: SchemaMetadata): void {
    const cacheKey = this.getCacheKey(metadata.namespace, metadata.name);
    const expiresAt = new Date(Date.now() + this.config.schemaCacheTtlMs);
    this.schemaCache.set(cacheKey, { metadata, expiresAt });
  }
}
