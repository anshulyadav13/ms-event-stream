import 'reflect-metadata';
import { SetMetadata } from '@nestjs/common';

/**
 * Metadata key for storing event handler information
 */
export const EVENT_HANDLER_METADATA = Symbol('EVENT_HANDLER_METADATA');

/**
 * Options for the @OnEvent decorator
 */
export interface OnEventOptions {
  /**
   * Priority for handler execution (higher = executed first)
   * @default 0
   */
  priority?: number;

  /**
   * Whether this handler should run in parallel with others
   * @default true
   */
  parallel?: boolean;

  /**
   * Custom retry policy for this handler
   */
  retryPolicy?: {
    /**
     * Maximum number of retry attempts
     * @default 3
     */
    maxAttempts?: number;

    /**
     * Base delay in milliseconds for exponential backoff
     * @default 1000
     */
    baseDelayMs?: number;

    /**
     * Maximum delay in milliseconds
     * @default 30000
     */
    maxDelayMs?: number;

    /**
     * Multiplier for exponential backoff
     * @default 2
     */
    backoffMultiplier?: number;

    /**
     * Enable jitter to prevent thundering herd
     * @default true
     */
    enableJitter?: boolean;

    /**
     * Jitter percentage (0-1)
     * @default 0.1
     */
    jitterPercent?: number;
  };

  /**
   * Timeout for handler execution in milliseconds
   * @default 30000
   */
  timeoutMs?: number;

  /**
   * Whether to validate the event payload against a schema
   * @default false
   */
  validatePayload?: boolean;

  /**
   * Custom event filter function
   * Return true to process the event, false to skip
   */
  filter?: (eventPayload: any) => boolean;

  /**
   * Tags for categorizing handlers (useful for monitoring)
   */
  tags?: string[];

  /**
   * Description of what this handler does (for documentation)
   */
  description?: string;
}

/**
 * Metadata stored for each event handler
 */
export interface EventHandlerDecoratorMetadata {
  /**
   * Event type this handler processes
   */
  eventType: string;

  /**
   * Method name that handles the event
   */
  methodName: string;

  /**
   * Handler options
   */
  options: OnEventOptions;

  /**
   * Target class constructor
   */
  target: any;
}

/**
 * @OnEvent decorator for declarative event handler registration
 * 
 * This decorator marks a method as an event handler and stores metadata
 * that will be discovered during module initialization.
 * 
 * @example
 * ```typescript
 * @Injectable()
 * class OrderService {
 *   @OnEvent('order.created')
 *   async handleOrderCreated(event: EventPayload) {
 *     // Handle order creation
 *   }
 * 
 *   @OnEvent('payment.completed', { priority: 10, parallel: false })
 *   async handlePaymentCompleted(event: EventPayload) {
 *     // Handle payment with high priority and sequential execution
 *   }
 * }
 * ```
 * 
 * @param eventType - The event type to listen for (e.g., "order.created")
 * @param options - Optional configuration for the handler
 */
export function OnEvent(eventType: string, options: OnEventOptions = {}): MethodDecorator {
  return function (target: any, propertyKey: string | symbol, descriptor: PropertyDescriptor) {
    // Validate parameters
    if (!eventType || typeof eventType !== 'string' || eventType.trim() === '') {
      throw new Error(`@OnEvent: eventType must be a non-empty string, got: ${eventType}`);
    }

    if (typeof propertyKey !== 'string') {
      throw new Error(`@OnEvent: can only be applied to string method names, got: ${typeof propertyKey}`);
    }

    if (!descriptor || typeof descriptor.value !== 'function') {
      throw new Error(`@OnEvent: can only be applied to methods, got: ${typeof descriptor.value}`);
    }

    // Validate options
    validateOnEventOptions(options);

    // Normalize event type (trim and lowercase)
    const normalizedEventType = eventType.trim();

    // Get existing metadata or create new array
    const existingMetadata: EventHandlerDecoratorMetadata[] = 
      Reflect.getMetadata(EVENT_HANDLER_METADATA, target.constructor) || [];

    // Check for duplicate handlers
    const duplicateHandler = existingMetadata.find(
      meta => meta.eventType === normalizedEventType && meta.methodName === propertyKey
    );

    if (duplicateHandler) {
      throw new Error(
        `@OnEvent: Duplicate handler for event '${normalizedEventType}' in method '${propertyKey}' of class '${target.constructor.name}'`
      );
    }

    // Create metadata for this handler
    const handlerMetadata: EventHandlerDecoratorMetadata = {
      eventType: normalizedEventType,
      methodName: propertyKey,
      options: {
        priority: 0,
        parallel: true,
        timeoutMs: 30000,
        validatePayload: false,
        ...options
      },
      target: target.constructor
    };

    // Add to metadata array
    existingMetadata.push(handlerMetadata);

    // Store updated metadata
    Reflect.defineMetadata(EVENT_HANDLER_METADATA, existingMetadata, target.constructor);

    // Also set NestJS metadata for potential integration with other decorators
    SetMetadata(EVENT_HANDLER_METADATA, existingMetadata)(target.constructor);

    // Log decorator application in development
    if (process.env.NODE_ENV !== 'production') {
      console.debug(
        `@OnEvent registered: ${target.constructor.name}.${propertyKey} -> '${normalizedEventType}' (priority: ${handlerMetadata.options.priority})`
      );
    }

    return descriptor;
  };
}

/**
 * Validate @OnEvent options
 */
function validateOnEventOptions(options: OnEventOptions): void {
  if (options.priority !== undefined) {
    if (typeof options.priority !== 'number' || options.priority < 0) {
      throw new Error('@OnEvent: priority must be a non-negative number');
    }
  }

  if (options.parallel !== undefined && typeof options.parallel !== 'boolean') {
    throw new Error('@OnEvent: parallel must be a boolean');
  }

  if (options.timeoutMs !== undefined) {
    if (typeof options.timeoutMs !== 'number' || options.timeoutMs <= 0) {
      throw new Error('@OnEvent: timeoutMs must be a positive number');
    }
  }

  if (options.validatePayload !== undefined && typeof options.validatePayload !== 'boolean') {
    throw new Error('@OnEvent: validatePayload must be a boolean');
  }

  if (options.filter !== undefined && typeof options.filter !== 'function') {
    throw new Error('@OnEvent: filter must be a function');
  }

  if (options.tags !== undefined) {
    if (!Array.isArray(options.tags) || !options.tags.every(tag => typeof tag === 'string')) {
      throw new Error('@OnEvent: tags must be an array of strings');
    }
  }

  if (options.description !== undefined && typeof options.description !== 'string') {
    throw new Error('@OnEvent: description must be a string');
  }

  // Validate retry policy if provided
  if (options.retryPolicy) {
    const { maxAttempts, baseDelayMs, backoffMultiplier, maxDelayMs } = options.retryPolicy;

    if (maxAttempts !== undefined) {
      if (typeof maxAttempts !== 'number' || maxAttempts < 0) {
        throw new Error('@OnEvent: retryPolicy.maxAttempts must be a non-negative number');
      }
    }

    if (baseDelayMs !== undefined) {
      if (typeof baseDelayMs !== 'number' || baseDelayMs <= 0) {
        throw new Error('@OnEvent: retryPolicy.baseDelayMs must be a positive number');
      }
    }

    if (backoffMultiplier !== undefined) {
      if (typeof backoffMultiplier !== 'number' || backoffMultiplier <= 0) {
        throw new Error('@OnEvent: retryPolicy.backoffMultiplier must be a positive number');
      }
    }

    if (maxDelayMs !== undefined) {
      if (typeof maxDelayMs !== 'number' || maxDelayMs <= 0) {
        throw new Error('@OnEvent: retryPolicy.maxDelayMs must be a positive number');
      }
    }
  }
}

/**
 * Get event handler metadata from a class
 * 
 * @param target - The class constructor to get metadata from
 * @returns Array of event handler metadata
 */
export function getEventHandlerMetadata(target: any): EventHandlerDecoratorMetadata[] {
  return Reflect.getMetadata(EVENT_HANDLER_METADATA, target) || [];
}

/**
 * Check if a class has any event handlers
 * 
 * @param target - The class constructor to check
 * @returns True if the class has event handlers
 */
export function hasEventHandlers(target: any): boolean {
  const metadata = getEventHandlerMetadata(target);
  return metadata.length > 0;
}

/**
 * Get event handlers for a specific event type from a class
 * 
 * @param target - The class constructor to check
 * @param eventType - The event type to filter by
 * @returns Array of handler metadata for the specified event type
 */
export function getEventHandlersForType(target: any, eventType: string): EventHandlerDecoratorMetadata[] {
  const metadata = getEventHandlerMetadata(target);
  return metadata.filter(meta => meta.eventType === eventType);
}

/**
 * Get all unique event types handled by a class
 * 
 * @param target - The class constructor to check
 * @returns Array of unique event types
 */
export function getHandledEventTypes(target: any): string[] {
  const metadata = getEventHandlerMetadata(target);
  const eventTypes = metadata.map(meta => meta.eventType);
  return [...new Set(eventTypes)];
}

/**
 * Utility function to create a typed event handler
 * This provides better TypeScript support when using the decorator
 * 
 * @example
 * ```typescript
 * interface OrderCreatedPayload {
 *   orderId: string;
 *   customerId: string;
 * }
 * 
 * @OnEvent('order.created')
 * async handleOrderCreated(event: TypedEventPayload<OrderCreatedPayload>) {
 *   // event.data is now properly typed as OrderCreatedPayload
 * }
 * ```
 */
export type TypedEventPayload<T = any> = {
  metadata: {
    eventId: string;
    eventType: string;
    createdAt: Date;
    correlationId?: string;
    traceId?: string;
    sourceService?: string;
    version?: string;
  };
  data: T;
};

/**
 * Helper function to create event type constants
 * This helps avoid typos and provides IDE autocomplete
 * 
 * @example
 * ```typescript
 * const Events = createEventTypes({
 *   ORDER_CREATED: 'order.created',
 *   ORDER_UPDATED: 'order.updated',
 *   PAYMENT_COMPLETED: 'payment.completed'
 * });
 * 
 * @OnEvent(Events.ORDER_CREATED)
 * async handleOrderCreated(event: EventPayload) {
 *   // Handler implementation
 * }
 * ```
 */
export function createEventTypes<T extends Record<string, string>>(eventTypes: T): T {
  // Validate event type format
  for (const [key, value] of Object.entries(eventTypes)) {
    if (typeof value !== 'string' || value.trim() === '') {
      throw new Error(`Invalid event type for key '${key}': must be a non-empty string`);
    }
    
    // Optional: validate event type format (e.g., must contain a dot)
    if (!/^[a-z][a-z0-9-]*\.[a-z][a-z0-9-]*$/i.test(value)) {
      console.warn(`Event type '${value}' does not follow recommended format 'domain.action'`);
    }
  }
  
  return eventTypes;
}

/**
 * Batch decorator for registering multiple event handlers with the same options
 * 
 * @example
 * ```typescript
 * @OnEvents(['order.created', 'order.updated'], { priority: 10 })
 * async handleOrderEvent(event: EventPayload) {
 *   // Handles both order.created and order.updated events
 * }
 * ```
 */
export function OnEvents(eventTypes: string[], options: OnEventOptions = {}): MethodDecorator {
  return function (target: any, propertyKey: string | symbol, descriptor: PropertyDescriptor) {
    // Validate parameters
    if (!Array.isArray(eventTypes) || eventTypes.length === 0) {
      throw new Error('@OnEvents: eventTypes must be a non-empty array');
    }

    // Apply @OnEvent decorator for each event type
    for (const eventType of eventTypes) {
      OnEvent(eventType, options)(target, propertyKey, descriptor);
    }

    return descriptor;
  };
}

/**
 * Conditional decorator for event handlers that should only run under certain conditions
 * 
 * @example
 * ```typescript
 * @OnEventIf('order.created', () => process.env.NODE_ENV === 'production')
 * async handleOrderCreatedInProduction(event: EventPayload) {
 *   // Only runs in production environment
 * }
 * ```
 */
export function OnEventIf(
  eventType: string, 
  condition: () => boolean, 
  options: OnEventOptions = {}
): MethodDecorator {
  const enhancedOptions: OnEventOptions = {
    ...options,
    filter: (eventPayload: any) => {
      // Check the condition first
      if (!condition()) {
        return false;
      }
      
      // If there's an existing filter, run it too
      if (options.filter) {
        return options.filter(eventPayload);
      }
      
      return true;
    }
  };

  return OnEvent(eventType, enhancedOptions);
}
