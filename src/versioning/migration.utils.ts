import { EventVersionMigrationStep, EventVersionMigrationStepType } from './interfaces';

/**
 * Utilities for event version migration
 */
export class MigrationUtils {
  /**
   * Create a migration step to add a field
   */
  static addField(field: string, value: any): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.ADD_FIELD,
      field,
      description: `Add new field ${field}`,
      newValue: value,
    };
  }

  /**
   * Create a migration step to remove a field
   */
  static removeField(field: string): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.REMOVE_FIELD,
      field,
      description: `Remove field ${field}`,
    };
  }

  /**
   * Create a migration step to rename a field
   */
  static renameField(oldField: string, newField: string): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.RENAME_FIELD,
      field: oldField,
      description: `Rename field ${oldField} to ${newField}`,
      newValue: newField,
    };
  }

  /**
   * Create a migration step to change field type
   */
  static changeType(field: string, oldType: string, newType: string): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.CHANGE_TYPE,
      field,
      description: `Change type of field ${field} from ${oldType} to ${newType}`,
      oldValue: oldType,
      newValue: newType,
    };
  }

  /**
   * Create a migration step to transform a field
   */
  static transformField(
    field: string,
    transform: (value: any) => any | Promise<any>
  ): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.TRANSFORM,
      field,
      description: `Transform field ${field}`,
      transform,
    };
  }

  /**
   * Create a migration step to move a field
   */
  static moveField(sourceField: string, targetField: string): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.MOVE_FIELD,
      field: sourceField,
      description: `Move field ${sourceField} to ${targetField}`,
      newValue: targetField,
    };
  }

  /**
   * Create a migration step to split a field
   */
  static splitField(
    sourceField: string,
    targetFields: Record<string, string>
  ): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.SPLIT_FIELD,
      field: sourceField,
      description: `Split field ${sourceField} into multiple fields`,
      newValue: targetFields,
    };
  }

  /**
   * Create a migration step to merge fields
   */
  static mergeFields(
    targetField: string,
    sourceFields: Record<string, string>
  ): EventVersionMigrationStep {
    return {
      type: EventVersionMigrationStepType.MERGE_FIELDS,
      field: targetField,
      description: `Merge multiple fields into ${targetField}`,
      oldValue: sourceFields,
    };
  }

  /**
   * Common field transformers
   */
  static transformers = {
    /**
     * Convert to string
     */
    toString: (value: any) => String(value),

    /**
     * Convert to number
     */
    toNumber: (value: any) => Number(value),

    /**
     * Convert to boolean
     */
    toBoolean: (value: any) => Boolean(value),

    /**
     * Convert to date
     */
    toDate: (value: any) => new Date(value),

    /**
     * Convert to array
     */
    toArray: (value: any) => Array.isArray(value) ? value : [value],

    /**
     * Convert to object
     */
    toObject: (value: any) => typeof value === 'object' ? value : { value },

    /**
     * Convert to ISO date string
     */
    toISOString: (value: any) => new Date(value).toISOString(),

    /**
     * Convert to Unix timestamp
     */
    toUnixTimestamp: (value: any) => new Date(value).getTime(),

    /**
     * Convert to lowercase string
     */
    toLowerCase: (value: any) => String(value).toLowerCase(),

    /**
     * Convert to uppercase string
     */
    toUpperCase: (value: any) => String(value).toUpperCase(),

    /**
     * Trim string
     */
    trim: (value: any) => String(value).trim(),

    /**
     * Parse JSON string
     */
    parseJSON: (value: any) => JSON.parse(value),

    /**
     * Stringify to JSON
     */
    stringifyJSON: (value: any) => JSON.stringify(value),

    /**
     * Round number
     */
    round: (value: any) => Math.round(Number(value)),

    /**
     * Floor number
     */
    floor: (value: any) => Math.floor(Number(value)),

    /**
     * Ceil number
     */
    ceil: (value: any) => Math.ceil(Number(value)),

    /**
     * Convert to fixed decimal places
     */
    toFixed: (decimals: number) => (value: any) => Number(value).toFixed(decimals),

    /**
     * Replace string
     */
    replace: (search: string | RegExp, replace: string) => (value: any) =>
      String(value).replace(search, replace),

    /**
     * Split string
     */
    split: (separator: string | RegExp) => (value: any) =>
      String(value).split(separator),

    /**
     * Join array
     */
    join: (separator: string) => (value: any) =>
      Array.isArray(value) ? value.join(separator) : value,

    /**
     * Map array
     */
    map: (fn: (item: any) => any) => (value: any) =>
      Array.isArray(value) ? value.map(fn) : value,

    /**
     * Filter array
     */
    filter: (fn: (item: any) => boolean) => (value: any) =>
      Array.isArray(value) ? value.filter(fn) : value,

    /**
     * Reduce array
     */
    reduce: (fn: (acc: any, item: any) => any, initial: any) => (value: any) =>
      Array.isArray(value) ? value.reduce(fn, initial) : value,

    /**
     * Pick object properties
     */
    pick: (props: string[]) => (value: any) => {
      if (typeof value !== 'object' || !value) {
        return value;
      }
      const result: Record<string, any> = {};
      for (const prop of props) {
        if (prop in value) {
          result[prop] = value[prop];
        }
      }
      return result;
    },

    /**
     * Omit object properties
     */
    omit: (props: string[]) => (value: any) => {
      if (typeof value !== 'object' || !value) {
        return value;
      }
      const result: Record<string, any> = { ...value };
      for (const prop of props) {
        delete result[prop];
      }
      return result;
    },

    /**
     * Rename object properties
     */
    renameProps: (mapping: Record<string, string>) => (value: any) => {
      if (typeof value !== 'object' || !value) {
        return value;
      }
      const result: Record<string, any> = { ...value };
      for (const [oldProp, newProp] of Object.entries(mapping)) {
        if (oldProp in result) {
          result[newProp] = result[oldProp];
          delete result[oldProp];
        }
      }
      return result;
    },

    /**
     * Transform object properties
     */
    transformProps: (mapping: Record<string, (value: any) => any>) => (value: any) => {
      if (typeof value !== 'object' || !value) {
        return value;
      }
      const result: Record<string, any> = { ...value };
      for (const [prop, transform] of Object.entries(mapping)) {
        if (prop in result) {
          result[prop] = transform(result[prop]);
        }
      }
      return result;
    },
  };
}
