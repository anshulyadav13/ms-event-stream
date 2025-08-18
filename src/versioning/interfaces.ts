/**
 * Event version strategy types
 */
export enum EventVersionStrategy {
  /**
   * Strict versioning - requires exact version match
   */
  STRICT = 'STRICT',

  /**
   * Backward compatible - newer versions can read older data
   */
  BACKWARD = 'BACKWARD',

  /**
   * Forward compatible - older versions can read newer data
   */
  FORWARD = 'FORWARD',

  /**
   * Full compatibility - both backward and forward compatible
   */
  FULL = 'FULL',

  /**
   * None - no version compatibility checking
   */
  NONE = 'NONE',
}

/**
 * Event migration strategy types
 */
export enum EventMigrationStrategy {
  /**
   * Automatic migration during publish/consume
   */
  AUTOMATIC = 'AUTOMATIC',

  /**
   * Manual migration through explicit API calls
   */
  MANUAL = 'MANUAL',

  /**
   * No migration - events stay in original version
   */
  NONE = 'NONE',
}

/**
 * Event version metadata
 */
export interface EventVersion {
  /**
   * Version number (semver)
   */
  version: string;

  /**
   * Event type
   */
  eventType: string;

  /**
   * Event schema for this version
   */
  schema: Record<string, any>;

  /**
   * Version description
   */
  description?: string | undefined;

  /**
   * Version status
   */
  status: EventVersionStatus;

  /**
   * Version compatibility level
   */
  compatibility: EventVersionStrategy;

  /**
   * Migration strategy for this version
   */
  migrationStrategy: EventMigrationStrategy;

  /**
   * Created timestamp
   */
  createdAt: Date;

  /**
   * Last updated timestamp
   */
  updatedAt: Date;

  /**
   * Deprecated timestamp
   */
  deprecatedAt?: Date | undefined;

  /**
   * End of life timestamp
   */
  endOfLifeAt?: Date | undefined;

  /**
   * Version tags
   */
  tags?: string[] | undefined;
}

/**
 * Event version status
 */
export enum EventVersionStatus {
  /**
   * Version is active and can be used
   */
  ACTIVE = 'ACTIVE',

  /**
   * Version is deprecated but still supported
   */
  DEPRECATED = 'DEPRECATED',

  /**
   * Version has reached end of life
   */
  END_OF_LIFE = 'END_OF_LIFE',
}

/**
 * Event version registration options
 */
export interface EventVersionRegistrationOptions {
  /**
   * Event type
   */
  eventType: string;

  /**
   * Version number (semver)
   */
  version: string;

  /**
   * Event schema for this version
   */
  schema: Record<string, any>;

  /**
   * Version description
   */
  description?: string;

  /**
   * Version compatibility level
   * @default EventVersionStrategy.BACKWARD
   */
  compatibility?: EventVersionStrategy;

  /**
   * Migration strategy for this version
   * @default EventMigrationStrategy.AUTOMATIC
   */
  migrationStrategy?: EventMigrationStrategy;

  /**
   * Version tags
   */
  tags?: string[];
}

/**
 * Event version update options
 */
export interface EventVersionUpdateOptions {
  /**
   * Event schema for this version
   */
  schema?: Record<string, any>;

  /**
   * Version description
   */
  description?: string;

  /**
   * Version compatibility level
   */
  compatibility?: EventVersionStrategy;

  /**
   * Migration strategy for this version
   */
  migrationStrategy?: EventMigrationStrategy;

  /**
   * Version tags to add
   */
  addTags?: string[];

  /**
   * Version tags to remove
   */
  removeTags?: string[];
}

/**
 * Event version compatibility result
 */
export interface EventVersionCompatibilityResult {
  /**
   * Whether versions are compatible
   */
  isCompatible: boolean;

  /**
   * Compatibility errors
   */
  errors?: string[] | undefined;

  /**
   * Compatibility warnings
   */
  warnings?: string[] | undefined;

  /**
   * Required migrations
   */
  requiredMigrations?: EventVersionMigration[] | undefined;
}

/**
 * Event version migration
 */
export interface EventVersionMigration {
  /**
   * Source version
   */
  sourceVersion: string;

  /**
   * Target version
   */
  targetVersion: string;

  /**
   * Migration steps
   */
  steps: EventVersionMigrationStep[];

  /**
   * Whether migration is breaking
   */
  isBreaking: boolean;

  /**
   * Migration description
   */
  description?: string;
}

/**
 * Event version migration step
 */
export interface EventVersionMigrationStep {
  /**
   * Step type
   */
  type: EventVersionMigrationStepType;

  /**
   * Field path
   */
  field: string;

  /**
   * Step description
   */
  description: string;

  /**
   * Old value (for transforms)
   */
  oldValue?: any;

  /**
   * New value (for transforms)
   */
  newValue?: any;

  /**
   * Custom transform function
   */
  transform?: (value: any) => any | Promise<any>;
}

/**
 * Event version migration step type
 */
export enum EventVersionMigrationStepType {
  /**
   * Add a new field
   */
  ADD_FIELD = 'ADD_FIELD',

  /**
   * Remove a field
   */
  REMOVE_FIELD = 'REMOVE_FIELD',

  /**
   * Rename a field
   */
  RENAME_FIELD = 'RENAME_FIELD',

  /**
   * Change field type
   */
  CHANGE_TYPE = 'CHANGE_TYPE',

  /**
   * Transform field value
   */
  TRANSFORM = 'TRANSFORM',

  /**
   * Move field to new location
   */
  MOVE_FIELD = 'MOVE_FIELD',

  /**
   * Split field into multiple fields
   */
  SPLIT_FIELD = 'SPLIT_FIELD',

  /**
   * Merge multiple fields into one
   */
  MERGE_FIELDS = 'MERGE_FIELDS',
}

/**
 * Event version validation result
 */
export interface EventVersionValidationResult {
  /**
   * Whether version is valid
   */
  isValid: boolean;

  /**
   * Validation errors
   */
  errors?: string[];

  /**
   * Validation warnings
   */
  warnings?: string[];
}

/**
 * Event version error
 */
export class EventVersionError extends Error {
  constructor(
    message: string,
    public readonly eventType: string,
    public readonly version: string,
    public readonly error?: Error
  ) {
    super(message);
    this.name = 'EventVersionError';
  }

  override readonly name: string;
}

/**
 * Event version validation error
 */
export class EventVersionValidationError extends Error {
  constructor(
    message: string,
    public readonly errors: string[],
    public readonly warnings?: string[]
  ) {
    super(message);
    this.name = 'EventVersionValidationError';
  }

  override readonly name: string;
}

/**
 * Event version not found error
 */
export class EventVersionNotFoundError extends Error {
  constructor(
    public readonly eventType: string,
    public readonly version: string
  ) {
    super(`Event version not found: ${eventType} v${version}`);
    this.name = 'EventVersionNotFoundError';
  }

  override readonly name: string;
}

/**
 * Event version already exists error
 */
export class EventVersionAlreadyExistsError extends Error {
  constructor(
    public readonly eventType: string,
    public readonly version: string
  ) {
    super(`Event version already exists: ${eventType} v${version}`);
    this.name = 'EventVersionAlreadyExistsError';
  }

  override readonly name: string;
}

/**
 * Event version operation not allowed error
 */
export class EventVersionOperationNotAllowedError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'EventVersionOperationNotAllowedError';
  }

  override readonly name: string;
}

/**
 * Event version compatibility error
 */
export class EventVersionCompatibilityError extends Error {
  constructor(
    message: string,
    public readonly sourceVersion: string,
    public readonly targetVersion: string,
    public readonly errors: string[]
  ) {
    super(message);
    this.name = 'EventVersionCompatibilityError';
  }

  override readonly name: string;
}

/**
 * Event version migration error
 */
export class EventVersionMigrationError extends Error {
  constructor(
    message: string,
    public readonly sourceVersion: string,
    public readonly targetVersion: string,
    public readonly error?: Error
  ) {
    super(message);
    this.name = 'EventVersionMigrationError';
  }

  override readonly name: string;
}
