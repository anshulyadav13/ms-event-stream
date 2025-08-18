import { EventVersionStrategy, EventVersionMigration, EventVersionMigrationStep, EventVersionMigrationStepType } from './interfaces';

/**
 * Checks compatibility between event versions
 */
export class VersionCompatibilityChecker {
  /**
   * Check compatibility between two schemas
   */
  static checkCompatibility(
    sourceSchema: Record<string, any>,
    targetSchema: Record<string, any>,
    strategy: EventVersionStrategy
  ): {
    isCompatible: boolean;
    errors: string[];
    warnings: string[];
    requiredMigrations: EventVersionMigration[];
  } {
    const errors: string[] = [];
    const warnings: string[] = [];
    const migrationSteps: EventVersionMigrationStep[] = [];

    // Compare schemas based on strategy
    switch (strategy) {
      case EventVersionStrategy.STRICT:
        this.checkStrictCompatibility(sourceSchema, targetSchema, errors, warnings, migrationSteps);
        break;

      case EventVersionStrategy.BACKWARD:
        this.checkBackwardCompatibility(sourceSchema, targetSchema, errors, warnings, migrationSteps);
        break;

      case EventVersionStrategy.FORWARD:
        this.checkForwardCompatibility(sourceSchema, targetSchema, errors, warnings, migrationSteps);
        break;

      case EventVersionStrategy.FULL:
        this.checkFullCompatibility(sourceSchema, targetSchema, errors, warnings, migrationSteps);
        break;

      case EventVersionStrategy.NONE:
        // No compatibility checking needed
        break;

      default:
        errors.push(`Unknown compatibility strategy: ${strategy}`);
    }

    // Create migration if needed
    const requiredMigrations: EventVersionMigration[] = migrationSteps.length > 0 ? [{
      sourceVersion: 'unknown',
      targetVersion: 'unknown',
      steps: migrationSteps,
      isBreaking: errors.length > 0,
      description: 'Schema migration required',
    }] : [];

    return {
      isCompatible: errors.length === 0,
      errors,
      warnings: warnings.length > 0 ? warnings : [],
      requiredMigrations,
    };
  }

  /**
   * Check strict compatibility (exact match)
   */
  private static checkStrictCompatibility(
    sourceSchema: Record<string, any>,
    targetSchema: Record<string, any>,
    errors: string[],
    warnings: string[],
    migrationSteps: EventVersionMigrationStep[]
  ): void {
    // Compare required fields
    const sourceRequired = new Set(sourceSchema.required || []);
    const targetRequired = new Set(targetSchema.required || []);

    if (sourceRequired.size !== targetRequired.size) {
      errors.push('Different number of required fields');
    }

    for (const field of sourceRequired) {
      if (!targetRequired.has(field)) {
        errors.push(`Required field ${field} removed`);
      }
    }

    for (const field of targetRequired) {
      if (!sourceRequired.has(field)) {
        errors.push(`New required field ${field} added`);
      }
    }

    // Compare properties
    const sourceProps = sourceSchema.properties || {};
    const targetProps = targetSchema.properties || {};

    for (const [field, sourceType] of Object.entries(sourceProps)) {
      const targetType = targetProps[field];

      if (!targetType) {
        errors.push(`Field ${field} removed`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.REMOVE_FIELD,
          field,
          description: `Remove field ${field}`,
        });
        continue;
      }

      if (!this.areTypesCompatible(sourceType as Record<string, any>, targetType as Record<string, any>)) {
        const sourceTypeObj = sourceType as Record<string, any>;
        const targetTypeObj = targetType as Record<string, any>;
        errors.push(`Field ${field} type changed from ${sourceTypeObj.type} to ${targetTypeObj.type}`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.CHANGE_TYPE,
          field,
          description: `Change type of field ${field} from ${sourceTypeObj.type} to ${targetTypeObj.type}`,
          oldValue: sourceTypeObj.type,
          newValue: targetTypeObj.type,
        });
      }
    }

    for (const [field, targetType] of Object.entries(targetProps)) {
      if (!sourceProps[field]) {
        errors.push(`New field ${field} added`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.ADD_FIELD,
          field,
          description: `Add new field ${field}`,
          newValue: (targetType as Record<string, any>).default,
        });
      }
    }
  }

  /**
   * Check backward compatibility (newer can read older)
   */
  private static checkBackwardCompatibility(
    sourceSchema: Record<string, any>,
    targetSchema: Record<string, any>,
    errors: string[],
    warnings: string[],
    migrationSteps: EventVersionMigrationStep[]
  ): void {
    // Compare required fields
    const sourceRequired = new Set(sourceSchema.required || []);
    const targetRequired = new Set(targetSchema.required || []);

    for (const field of targetRequired) {
      if (!sourceRequired.has(field)) {
        errors.push(`New required field ${field} breaks backward compatibility`);
      }
    }

    // Compare properties
    const sourceProps = sourceSchema.properties || {};
    const targetProps = targetSchema.properties || {};

    for (const [field, sourceType] of Object.entries(sourceProps)) {
      const targetType = targetProps[field];

      if (!targetType) {
        warnings.push(`Field ${field} removed, but allowed in backward compatibility`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.REMOVE_FIELD,
          field,
          description: `Remove field ${field}`,
        });
        continue;
      }

      if (!this.areTypesCompatible(sourceType as Record<string, any>, targetType as Record<string, any>)) {
        const sourceTypeObj = sourceType as Record<string, any>;
        const targetTypeObj = targetType as Record<string, any>;
        errors.push(`Field ${field} type change breaks backward compatibility`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.CHANGE_TYPE,
          field,
          description: `Change type of field ${field} from ${sourceTypeObj.type} to ${targetTypeObj.type}`,
          oldValue: sourceTypeObj.type,
          newValue: targetTypeObj.type,
        });
      }
    }

    for (const [field, targetType] of Object.entries(targetProps)) {
      if (!sourceProps[field]) {
        warnings.push(`New optional field ${field} added`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.ADD_FIELD,
          field,
          description: `Add new field ${field}`,
          newValue: (targetType as Record<string, any>).default,
        });
      }
    }
  }

  /**
   * Check forward compatibility (older can read newer)
   */
  private static checkForwardCompatibility(
    sourceSchema: Record<string, any>,
    targetSchema: Record<string, any>,
    errors: string[],
    warnings: string[],
    migrationSteps: EventVersionMigrationStep[]
  ): void {
    // Compare required fields
    const sourceRequired = new Set(sourceSchema.required || []);
    const targetRequired = new Set(targetSchema.required || []);

    for (const field of sourceRequired) {
      if (!targetRequired.has(field)) {
        errors.push(`Required field ${field} removal breaks forward compatibility`);
      }
    }

    // Compare properties
    const sourceProps = sourceSchema.properties || {};
    const targetProps = targetSchema.properties || {};

    for (const [field, sourceType] of Object.entries(sourceProps)) {
      const targetType = targetProps[field];

      if (!targetType) {
        errors.push(`Field ${field} removal breaks forward compatibility`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.REMOVE_FIELD,
          field,
          description: `Remove field ${field}`,
        });
        continue;
      }

      if (!this.areTypesCompatible(sourceType as Record<string, any>, targetType as Record<string, any>)) {
        const sourceTypeObj = sourceType as Record<string, any>;
        const targetTypeObj = targetType as Record<string, any>;
        errors.push(`Field ${field} type change breaks forward compatibility`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.CHANGE_TYPE,
          field,
          description: `Change type of field ${field} from ${sourceTypeObj.type} to ${targetTypeObj.type}`,
          oldValue: sourceTypeObj.type,
          newValue: targetTypeObj.type,
        });
      }
    }

    for (const [field, targetType] of Object.entries(targetProps)) {
      if (!sourceProps[field]) {
        warnings.push(`New field ${field} added, but allowed in forward compatibility`);
        migrationSteps.push({
          type: EventVersionMigrationStepType.ADD_FIELD,
          field,
          description: `Add new field ${field}`,
          newValue: (targetType as Record<string, any>).default,
        });
      }
    }
  }

  /**
   * Check full compatibility (both backward and forward)
   */
  private static checkFullCompatibility(
    sourceSchema: Record<string, any>,
    targetSchema: Record<string, any>,
    errors: string[],
    warnings: string[],
    migrationSteps: EventVersionMigrationStep[]
  ): void {
    // Check both backward and forward compatibility
    this.checkBackwardCompatibility(sourceSchema, targetSchema, errors, warnings, migrationSteps);
    this.checkForwardCompatibility(sourceSchema, targetSchema, errors, warnings, migrationSteps);
  }

  /**
   * Check if types are compatible
   */
  private static areTypesCompatible(
    sourceType: Record<string, any>,
    targetType: Record<string, any>
  ): boolean {
    // Handle type arrays
    const sourceTypes = Array.isArray(sourceType.type) ? sourceType.type : [sourceType.type];
    const targetTypes = Array.isArray(targetType.type) ? targetType.type : [targetType.type];

    // Check if all source types are included in target types
    return sourceTypes.every(type => targetTypes.includes(type));
  }
}
