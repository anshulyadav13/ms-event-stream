import { JSONSchema7 } from 'json-schema';

/**
 * Schema version information
 */
export interface SchemaVersion {
  version: number;
  schema: JSONSchema7;
  createdAt: Date;
  deprecated?: boolean | undefined;
  deprecatedAt?: Date | undefined;
  description?: string | undefined;
}

/**
 * Schema metadata
 */
export interface SchemaMetadata {
  name: string;
  namespace: string;
  currentVersion: number;
  versions: SchemaVersion[];
  createdAt: Date;
  updatedAt: Date;
  description?: string | undefined;
  tags?: string[] | undefined;
}

/**
 * Schema compatibility level
 */
export enum SchemaCompatibilityLevel {
  NONE = 'NONE',                   // No compatibility checking
  BACKWARD = 'BACKWARD',           // New schema can read old data
  FORWARD = 'FORWARD',             // Old schema can read new data
  FULL = 'FULL',                   // Both backward and forward compatible
  BACKWARD_TRANSITIVE = 'BACKWARD_TRANSITIVE', // Backward compatible with all previous versions
  FORWARD_TRANSITIVE = 'FORWARD_TRANSITIVE',   // Forward compatible with all previous versions
  FULL_TRANSITIVE = 'FULL_TRANSITIVE'         // Both backward and forward compatible with all versions
}

/**
 * Schema validation result
 */
export interface SchemaValidationResult {
  isValid: boolean;
  errors?: string[] | undefined;
  warnings?: string[] | undefined;
}

/**
 * Schema compatibility check result
 */
export interface SchemaCompatibilityResult {
  isCompatible: boolean;
  level: SchemaCompatibilityLevel;
  errors?: string[] | undefined;
  warnings?: string[] | undefined;
}

/**
 * Schema evolution strategy
 */
export enum SchemaEvolutionStrategy {
  STRICT = 'STRICT',           // No breaking changes allowed
  PERMISSIVE = 'PERMISSIVE',   // Allow some breaking changes with warnings
  FLEXIBLE = 'FLEXIBLE'        // Allow most changes with warnings
}

/**
 * Schema registry configuration
 */
export interface SchemaRegistryConfig {
  enabled: boolean;
  enforceValidation: boolean;
  defaultCompatibilityLevel: SchemaCompatibilityLevel;
  evolutionStrategy: SchemaEvolutionStrategy;
  allowSchemaDeletion: boolean;
  maxVersionsPerSchema: number;
  enableSchemaCache: boolean;
  schemaCacheTtlMs: number;
  validateOnPublish: boolean;
  validateOnConsume: boolean;
  enableCompatibilityCheck: boolean;
  enableDeprecation: boolean;
  enableVersioning: boolean;
  enableTags: boolean;
}

/**
 * Schema registration options
 */
export interface SchemaRegistrationOptions {
  name: string;
  namespace: string;
  schema: JSONSchema7;
  version?: number;
  description?: string;
  tags?: string[];
  compatibilityLevel?: SchemaCompatibilityLevel;
  evolutionStrategy?: SchemaEvolutionStrategy;
}

/**
 * Schema update options
 */
export interface SchemaUpdateOptions {
  schema: JSONSchema7;
  description?: string;
  tags?: string[];
  compatibilityLevel?: SchemaCompatibilityLevel;
  evolutionStrategy?: SchemaEvolutionStrategy;
}

/**
 * Schema search criteria
 */
export interface SchemaSearchCriteria {
  namespace?: string;
  name?: string;
  version?: number;
  tags?: string[];
  deprecated?: boolean;
  createdAfter?: Date;
  createdBefore?: Date;
  updatedAfter?: Date;
  updatedBefore?: Date;
}

/**
 * Schema cache entry
 */
export interface SchemaCacheEntry {
  metadata: SchemaMetadata;
  expiresAt: Date;
}

/**
 * Schema migration plan
 */
export interface SchemaMigrationPlan {
  sourceVersion: number;
  targetVersion: number;
  steps: SchemaMigrationStep[];
  isBreaking: boolean;
  warnings?: string[] | undefined;
}

/**
 * Schema migration step
 */
export interface SchemaMigrationStep {
  type: 'ADD_FIELD' | 'REMOVE_FIELD' | 'MODIFY_FIELD' | 'RENAME_FIELD';
  field: string;
  description: string;
  isBreaking: boolean;
}

/**
 * Schema validation error
 */
export class SchemaValidationError extends Error {
  constructor(
    message: string,
    public readonly errors: string[],
    public readonly warnings?: string[]
  ) {
    super(message);
    this.name = 'SchemaValidationError';
  }
}

/**
 * Schema compatibility error
 */
export class SchemaCompatibilityError extends Error {
  constructor(
    message: string,
    public readonly errors: string[],
    public readonly warnings?: string[]
  ) {
    super(message);
    this.name = 'SchemaCompatibilityError';
  }
}

/**
 * Schema not found error
 */
export class SchemaNotFoundError extends Error {
  constructor(
    public readonly namespace: string,
    public override readonly name: string,
    public readonly version?: number
  ) {
    super(`Schema not found: ${namespace}/${name}${version ? `@${version}` : ''}`);
    this.name = 'SchemaNotFoundError';
  }
}

/**
 * Schema already exists error
 */
export class SchemaAlreadyExistsError extends Error {
  constructor(
    public readonly namespace: string,
    public override readonly name: string,
    public readonly version?: number
  ) {
    super(`Schema already exists: ${namespace}/${name}${version ? `@${version}` : ''}`);
    this.name = 'SchemaAlreadyExistsError';
  }
}

/**
 * Schema version mismatch error
 */
export class SchemaVersionMismatchError extends Error {
  constructor(
    public readonly namespace: string,
    public override readonly name: string,
    public readonly expectedVersion: number,
    public readonly actualVersion: number
  ) {
    super(`Schema version mismatch: ${namespace}/${name} expected v${expectedVersion}, got v${actualVersion}`);
    this.name = 'SchemaVersionMismatchError';
  }
}

/**
 * Schema operation not allowed error
 */
export class SchemaOperationNotAllowedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SchemaOperationNotAllowedError';
  }

  override readonly name: string;
}
