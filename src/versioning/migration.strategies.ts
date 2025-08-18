import {
  EventVersionMigrationStep,
  EventVersionMigrationStepType,
  EventVersionMigrationError,
} from './interfaces';

/**
 * Base class for event migration strategies
 */
export abstract class EventMigrationStrategy {
  /**
   * Apply migration steps to event data
   */
  abstract apply(data: any, steps: EventVersionMigrationStep[]): Promise<any>;
}

/**
 * Automatic event migration strategy
 */
export class AutomaticMigrationStrategy extends EventMigrationStrategy {
  /**
   * Apply migration steps to event data
   */
  async apply(data: any, steps: EventVersionMigrationStep[]): Promise<any> {
    let result = { ...data };

    for (const step of steps) {
      try {
        result = await this.applyStep(result, step);
      } catch (error) {
        throw new EventVersionMigrationError(
          `Failed to apply migration step: ${error instanceof Error ? error.message : String(error)}`,
          'unknown',
          'unknown',
          error instanceof Error ? error : undefined
        );
      }
    }

    return result;
  }

  /**
   * Apply a single migration step
   */
  private async applyStep(data: any, step: EventVersionMigrationStep): Promise<any> {
    switch (step.type) {
      case EventVersionMigrationStepType.ADD_FIELD:
        return this.addField(data, step);

      case EventVersionMigrationStepType.REMOVE_FIELD:
        return this.removeField(data, step);

      case EventVersionMigrationStepType.RENAME_FIELD:
        return this.renameField(data, step);

      case EventVersionMigrationStepType.CHANGE_TYPE:
        return this.changeType(data, step);

      case EventVersionMigrationStepType.TRANSFORM:
        return this.transform(data, step);

      case EventVersionMigrationStepType.MOVE_FIELD:
        return this.moveField(data, step);

      case EventVersionMigrationStepType.SPLIT_FIELD:
        return this.splitField(data, step);

      case EventVersionMigrationStepType.MERGE_FIELDS:
        return this.mergeFields(data, step);

      default:
        throw new Error(`Unknown migration step type: ${step.type}`);
    }
  }

  /**
   * Add a new field
   */
  private addField(data: any, step: EventVersionMigrationStep): any {
    const result = { ...data };
    const path = step.field.split('.');
    let current = result;

    for (let i = 0; i < path.length - 1; i++) {
      const part = path[i];
      if (!(part in current)) {
        current[part] = {};
      }
      current = current[part];
    }

    current[path[path.length - 1]] = step.newValue;
    return result;
  }

  /**
   * Remove a field
   */
  private removeField(data: any, step: EventVersionMigrationStep): any {
    const result = { ...data };
    const path = step.field.split('.');
    let current = result;

    for (let i = 0; i < path.length - 1; i++) {
      const part = path[i];
      if (!(part in current)) {
        return result;
      }
      current = current[part];
    }

    delete current[path[path.length - 1]];
    return result;
  }

  /**
   * Rename a field
   */
  private renameField(data: any, step: EventVersionMigrationStep): any {
    const result = { ...data };
    const oldPath = step.field.split('.');
    const newPath = (step.newValue as string).split('.');
    let oldCurrent = result;
    let newCurrent = result;

    // Get old value
    for (let i = 0; i < oldPath.length - 1; i++) {
      const part = oldPath[i];
      if (!(part in oldCurrent)) {
        return result;
      }
      oldCurrent = oldCurrent[part];
    }

    const value = oldCurrent[oldPath[oldPath.length - 1]];
    if (value === undefined) {
      return result;
    }

    // Create new path
    for (let i = 0; i < newPath.length - 1; i++) {
      const part = newPath[i];
      if (!(part in newCurrent)) {
        newCurrent[part] = {};
      }
      newCurrent = newCurrent[part];
    }

    // Set new value and delete old
    newCurrent[newPath[newPath.length - 1]] = value;
    delete oldCurrent[oldPath[oldPath.length - 1]];

    return result;
  }

  /**
   * Change field type
   */
  private async changeType(data: any, step: EventVersionMigrationStep): Promise<any> {
    const result = { ...data };
    const path = step.field.split('.');
    let current = result;

    // Get value
    for (let i = 0; i < path.length - 1; i++) {
      const part = path[i];
      if (!(part in current)) {
        return result;
      }
      current = current[part];
    }

    const value = current[path[path.length - 1]];
    if (value === undefined) {
      return result;
    }

    // Transform value based on target type
    const targetType = step.newValue as string;
    switch (targetType) {
      case 'string':
        current[path[path.length - 1]] = String(value);
        break;

      case 'number':
        current[path[path.length - 1]] = Number(value);
        break;

      case 'boolean':
        current[path[path.length - 1]] = Boolean(value);
        break;

      case 'date':
        current[path[path.length - 1]] = new Date(value);
        break;

      case 'array':
        current[path[path.length - 1]] = Array.isArray(value) ? value : [value];
        break;

      case 'object':
        current[path[path.length - 1]] = typeof value === 'object' ? value : { value };
        break;

      default:
        throw new Error(`Unsupported target type: ${targetType}`);
    }

    return result;
  }

  /**
   * Transform field value
   */
  private async transform(data: any, step: EventVersionMigrationStep): Promise<any> {
    const result = { ...data };
    const path = step.field.split('.');
    let current = result;

    // Get value
    for (let i = 0; i < path.length - 1; i++) {
      const part = path[i];
      if (!(part in current)) {
        return result;
      }
      current = current[part];
    }

    const value = current[path[path.length - 1]];
    if (value === undefined) {
      return result;
    }

    // Apply transform function
    if (step.transform) {
      current[path[path.length - 1]] = await step.transform(value);
    }

    return result;
  }

  /**
   * Move field to new location
   */
  private moveField(data: any, step: EventVersionMigrationStep): any {
    const result = { ...data };
    const sourcePath = step.field.split('.');
    const targetPath = (step.newValue as string).split('.');
    let sourceCurrent = result;
    let targetCurrent = result;

    // Get source value
    for (let i = 0; i < sourcePath.length - 1; i++) {
      const part = sourcePath[i];
      if (!(part in sourceCurrent)) {
        return result;
      }
      sourceCurrent = sourceCurrent[part];
    }

    const value = sourceCurrent[sourcePath[sourcePath.length - 1]];
    if (value === undefined) {
      return result;
    }

    // Create target path
    for (let i = 0; i < targetPath.length - 1; i++) {
      const part = targetPath[i];
      if (!(part in targetCurrent)) {
        targetCurrent[part] = {};
      }
      targetCurrent = targetCurrent[part];
    }

    // Move value
    targetCurrent[targetPath[targetPath.length - 1]] = value;
    delete sourceCurrent[sourcePath[sourcePath.length - 1]];

    return result;
  }

  /**
   * Split field into multiple fields
   */
  private splitField(data: any, step: EventVersionMigrationStep): any {
    const result = { ...data };
    const sourcePath = step.field.split('.');
    let current = result;

    // Get source value
    for (let i = 0; i < sourcePath.length - 1; i++) {
      const part = sourcePath[i];
      if (!(part in current)) {
        return result;
      }
      current = current[part];
    }

    const value = current[sourcePath[sourcePath.length - 1]];
    if (value === undefined) {
      return result;
    }

    // Split value into target fields
    const targetFields = step.newValue as Record<string, string>;
    for (const [targetField, path] of Object.entries(targetFields)) {
      const targetPath = path.split('.');
      let targetCurrent = result;

      // Create target path
      for (let i = 0; i < targetPath.length - 1; i++) {
        const part = targetPath[i];
        if (!(part in targetCurrent)) {
          targetCurrent[part] = {};
        }
        targetCurrent = targetCurrent[part];
      }

      // Set target value
      targetCurrent[targetPath[targetPath.length - 1]] = value[targetField];
    }

    // Remove source field
    delete current[sourcePath[sourcePath.length - 1]];

    return result;
  }

  /**
   * Merge multiple fields into one
   */
  private mergeFields(data: any, step: EventVersionMigrationStep): any {
    const result = { ...data };
    const targetPath = step.field.split('.');
    let targetCurrent = result;

    // Create target path
    for (let i = 0; i < targetPath.length - 1; i++) {
      const part = targetPath[i];
      if (!(part in targetCurrent)) {
        targetCurrent[part] = {};
      }
      targetCurrent = targetCurrent[part];
    }

    // Merge source fields into target
    const sourceFields = step.oldValue as Record<string, string>;
    const mergedValue: Record<string, any> = {};

    for (const [targetField, sourcePath] of Object.entries(sourceFields)) {
      const path = sourcePath.split('.');
      let current = result;

      // Get source value
      for (let i = 0; i < path.length - 1; i++) {
        const part = path[i];
        if (!(part in current)) {
          continue;
        }
        current = current[part];
      }

      const value = current[path[path.length - 1]];
      if (value !== undefined) {
        mergedValue[targetField] = value;
        delete current[path[path.length - 1]];
      }
    }

    // Set merged value
    targetCurrent[targetPath[targetPath.length - 1]] = mergedValue;

    return result;
  }
}

/**
 * Manual event migration strategy
 */
export class ManualMigrationStrategy extends EventMigrationStrategy {
  /**
   * Apply migration steps to event data
   */
  async apply(data: any, steps: EventVersionMigrationStep[]): Promise<any> {
    throw new EventVersionMigrationError(
      'Manual migration strategy requires explicit migration handling',
      'unknown',
      'unknown'
    );
  }
}

/**
 * No-op event migration strategy
 */
export class NoopMigrationStrategy extends EventMigrationStrategy {
  /**
   * Apply migration steps to event data
   */
  async apply(data: any, steps: EventVersionMigrationStep[]): Promise<any> {
    return data;
  }
}

/**
 * Factory for creating migration strategies
 */
export class EventMigrationStrategyFactory {
  /**
   * Create migration strategy instance
   */
  static create(type: string): EventMigrationStrategy {
    switch (type) {
      case 'AUTOMATIC':
        return new AutomaticMigrationStrategy();

      case 'MANUAL':
        return new ManualMigrationStrategy();

      case 'NONE':
        return new NoopMigrationStrategy();

      default:
        throw new Error(`Unknown migration strategy type: ${type}`);
    }
  }
}
