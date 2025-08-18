import { Injectable, Logger } from '@nestjs/common';
import { EventHandlerMetadata, EventPayload } from '../interfaces/event.interface';

/**
 * Handler execution result
 */
export interface HandlerExecutionResult {
  success: boolean;
  handlerName: string;
  executionTimeMs: number;
  error?: string;
  retryable?: boolean;
}

/**
 * Handler execution context
 */
export interface HandlerExecutionContext {
  eventId: string;
  eventType: string;
  correlationId?: string | undefined;
  traceId?: string | undefined;
  attempt: number;
  maxAttempts: number;
}

/**
 * Handler statistics
 */
export interface HandlerStats {
  totalExecutions: number;
  successfulExecutions: number;
  failedExecutions: number;
  averageExecutionTimeMs: number;
  lastExecutedAt?: Date | undefined;
  lastError?: string | undefined;
  lastErrorAt?: Date | undefined;
}

/**
 * Registry for managing event handlers with discovery and execution capabilities
 * 
 * Features:
 * - Handler registration and discovery
 * - Priority-based execution ordering
 * - Parallel vs sequential execution
 * - Error isolation between handlers
 * - Handler statistics and monitoring
 */
@Injectable()
export class HandlerRegistry {
  private readonly logger = new Logger(HandlerRegistry.name);
  private readonly eventHandlers = new Map<string, EventHandlerMetadata[]>();
  private readonly handlerStats = new Map<string, HandlerStats>();

  /**
   * Register an event handler
   * This is typically called by the @OnEvent decorator or during module initialization
   */
  registerHandler(metadata: EventHandlerMetadata): void {
    const { eventType, target, methodName } = metadata;
    
    this.logger.debug(`Registering handler: ${target.constructor.name}.${methodName} for event: ${eventType}`);

    // Validate handler metadata
    this.validateHandlerMetadata(metadata);

    // Get or create handler list for this event type
    if (!this.eventHandlers.has(eventType)) {
      this.eventHandlers.set(eventType, []);
    }

    const handlers = this.eventHandlers.get(eventType)!;
    
    // Check for duplicate handlers
    const existingHandler = handlers.find(
      h => h.target === target && h.methodName === methodName
    );
    
    if (existingHandler) {
      this.logger.warn(`Handler already registered: ${target.constructor.name}.${methodName} for event: ${eventType}`);
      return;
    }

    // Add handler to registry
    handlers.push(metadata);

    // Sort handlers by priority (higher priority first)
    handlers.sort((a, b) => (b.options?.priority || 0) - (a.options?.priority || 0));

    // Initialize statistics for this handler
    const handlerKey = this.getHandlerKey(metadata);
    this.handlerStats.set(handlerKey, {
      totalExecutions: 0,
      successfulExecutions: 0,
      failedExecutions: 0,
      averageExecutionTimeMs: 0
    });

    this.logger.log(`Handler registered: ${target.constructor.name}.${methodName} for event: ${eventType} (priority: ${metadata.options?.priority || 0})`);
  }

  /**
   * Unregister an event handler
   */
  unregisterHandler(eventType: string, target: any, methodName: string): boolean {
    const handlers = this.eventHandlers.get(eventType);
    if (!handlers) {
      return false;
    }

    const initialLength = handlers.length;
    const filteredHandlers = handlers.filter(
      h => !(h.target === target && h.methodName === methodName)
    );

    if (filteredHandlers.length === initialLength) {
      return false; // Handler not found
    }

    this.eventHandlers.set(eventType, filteredHandlers);

    // Clean up statistics
    const handlerKey = `${target.constructor.name}.${methodName}`;
    this.handlerStats.delete(handlerKey);

    this.logger.log(`Handler unregistered: ${target.constructor.name}.${methodName} for event: ${eventType}`);
    return true;
  }

  /**
   * Get all registered handlers for an event type
   */
  getHandlers(eventType: string): EventHandlerMetadata[] {
    return this.eventHandlers.get(eventType) || [];
  }

  /**
   * Get all registered event types
   */
  getRegisteredEventTypes(): string[] {
    return Array.from(this.eventHandlers.keys());
  }

  /**
   * Get total number of registered handlers
   */
  getTotalHandlerCount(): number {
    let total = 0;
    for (const handlers of this.eventHandlers.values()) {
      total += handlers.length;
    }
    return total;
  }

  /**
   * Get handlers count by event type
   */
  getHandlerCounts(): Map<string, number> {
    const counts = new Map<string, number>();
    for (const [eventType, handlers] of this.eventHandlers.entries()) {
      counts.set(eventType, handlers.length);
    }
    return counts;
  }

  /**
   * Execute all handlers for an event with proper error isolation
   */
  async executeHandlers(eventPayload: EventPayload): Promise<HandlerExecutionResult[]> {
    const { eventType } = eventPayload.metadata;
    const handlers = this.getHandlers(eventType);

    if (handlers.length === 0) {
      this.logger.warn(`No handlers registered for event type: ${eventType}`);
      return [];
    }

    this.logger.debug(`Executing ${handlers.length} handlers for event: ${eventType} (${eventPayload.metadata.eventId})`);

    const results: HandlerExecutionResult[] = [];
    const context: HandlerExecutionContext = {
      eventId: eventPayload.metadata.eventId,
      eventType: eventPayload.metadata.eventType,
      correlationId: eventPayload.metadata.correlationId,
      traceId: eventPayload.metadata.traceId,
      attempt: 1, // TODO: Phase 8 - Implement retry logic
      maxAttempts: 3 // TODO: Phase 8 - Make configurable
    };

    try {
      // Separate handlers by execution mode
      const parallelHandlers = handlers.filter(h => h.options?.parallel !== false);
      const sequentialHandlers = handlers.filter(h => h.options?.parallel === false);

      // Execute parallel handlers concurrently
      if (parallelHandlers.length > 0) {
        this.logger.debug(`Executing ${parallelHandlers.length} parallel handlers for event: ${eventType}`);
        
        const parallelPromises = parallelHandlers.map(handler => 
          this.executeHandler(handler, eventPayload, context)
        );
        
        const parallelResults = await Promise.allSettled(parallelPromises);
        
        parallelResults.forEach((result, index) => {
          if (result.status === 'fulfilled') {
            results.push(result.value);
          } else {
            const handler = parallelHandlers[index];
            const handlerName = this.getHandlerName(handler);
            
            results.push({
              success: false,
              handlerName,
              executionTimeMs: 0,
              error: result.reason instanceof Error ? result.reason.message : String(result.reason),
              retryable: true
            });
            
            this.logger.error(`Parallel handler execution failed: ${handlerName}`, result.reason);
          }
        });
      }

      // Execute sequential handlers one by one
      if (sequentialHandlers.length > 0) {
        this.logger.debug(`Executing ${sequentialHandlers.length} sequential handlers for event: ${eventType}`);
        
        for (const handler of sequentialHandlers) {
          try {
            const result = await this.executeHandler(handler, eventPayload, context);
            results.push(result);
            
            // If a sequential handler fails and is not retryable, stop execution
            if (!result.success && !result.retryable) {
              this.logger.error(`Sequential handler failed (non-retryable), stopping execution: ${result.handlerName}`);
              break;
            }
          } catch (error) {
            const handlerName = this.getHandlerName(handler);
            const errorMessage = error instanceof Error ? error.message : String(error);
            
            results.push({
              success: false,
              handlerName,
              executionTimeMs: 0,
              error: errorMessage,
              retryable: true
            });
            
            this.logger.error(`Sequential handler execution failed: ${handlerName}`, error);
            
            // Continue with next sequential handler even if current one fails
          }
        }
      }

      const successCount = results.filter(r => r.success).length;
      const failureCount = results.filter(r => !r.success).length;
      
      this.logger.debug(`Handler execution completed for event: ${eventType} (${eventPayload.metadata.eventId}) - Success: ${successCount}, Failed: ${failureCount}`);

      return results;

    } catch (error) {
      this.logger.error(`Unexpected error during handler execution for event: ${eventType}`, error);
      
      // Return error result for all handlers
      return handlers.map(handler => ({
        success: false,
        handlerName: this.getHandlerName(handler),
        executionTimeMs: 0,
        error: error instanceof Error ? error.message : String(error),
        retryable: true
      }));
    }
  }

  /**
   * Execute a single handler with timing and error handling
   */
  private async executeHandler(
    handler: EventHandlerMetadata,
    eventPayload: EventPayload,
    context: HandlerExecutionContext
  ): Promise<HandlerExecutionResult> {
    const handlerName = this.getHandlerName(handler);
    const handlerKey = this.getHandlerKey(handler);
    const startTime = Date.now();

    try {
      this.logger.debug(`Executing handler: ${handlerName} for event: ${context.eventType} (${context.eventId})`);

      // Execute the handler function
      await handler.handler(eventPayload);

      const executionTime = Date.now() - startTime;
      
      // Update statistics
      this.updateHandlerStats(handlerKey, true, executionTime);
      
      this.logger.debug(`Handler executed successfully: ${handlerName} in ${executionTime}ms`);

      return {
        success: true,
        handlerName,
        executionTimeMs: executionTime
      };

    } catch (error) {
      const executionTime = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      const isRetryable = this.isRetryableError(error);

      // Update statistics
      this.updateHandlerStats(handlerKey, false, executionTime, errorMessage);

      this.logger.error(`Handler execution failed: ${handlerName} in ${executionTime}ms`, {
        error: errorMessage,
        retryable: isRetryable,
        eventType: context.eventType,
        eventId: context.eventId
      });

      return {
        success: false,
        handlerName,
        executionTimeMs: executionTime,
        error: errorMessage,
        retryable: isRetryable
      };
    }
  }

  /**
   * Update handler statistics
   */
  private updateHandlerStats(
    handlerKey: string,
    success: boolean,
    executionTimeMs: number,
    error?: string
  ): void {
    const stats = this.handlerStats.get(handlerKey);
    if (!stats) {
      return;
    }

    stats.totalExecutions++;
    stats.lastExecutedAt = new Date();

    if (success) {
      stats.successfulExecutions++;
    } else {
      stats.failedExecutions++;
      stats.lastError = error;
      stats.lastErrorAt = new Date();
    }

    // Update average execution time using exponential moving average
    if (stats.averageExecutionTimeMs === 0) {
      stats.averageExecutionTimeMs = executionTimeMs;
    } else {
      // Use exponential moving average with alpha = 0.1
      stats.averageExecutionTimeMs = 
        (0.9 * stats.averageExecutionTimeMs) + (0.1 * executionTimeMs);
    }
  }

  /**
   * Get handler statistics
   */
  getHandlerStats(eventType?: string): Map<string, HandlerStats> {
    if (!eventType) {
      return new Map(this.handlerStats);
    }

    const filteredStats = new Map<string, HandlerStats>();
    const handlers = this.getHandlers(eventType);
    
    for (const handler of handlers) {
      const handlerKey = this.getHandlerKey(handler);
      const stats = this.handlerStats.get(handlerKey);
      if (stats) {
        filteredStats.set(handlerKey, { ...stats });
      }
    }

    return filteredStats;
  }

  /**
   * Reset statistics for a specific handler or all handlers
   */
  resetStats(handlerKey?: string): void {
    if (handlerKey) {
      const stats = this.handlerStats.get(handlerKey);
      if (stats) {
        Object.assign(stats, {
          totalExecutions: 0,
          successfulExecutions: 0,
          failedExecutions: 0,
          averageExecutionTimeMs: 0,
          lastExecutedAt: undefined,
          lastError: undefined,
          lastErrorAt: undefined
        });
        this.logger.log(`Statistics reset for handler: ${handlerKey}`);
      }
    } else {
      for (const stats of this.handlerStats.values()) {
        Object.assign(stats, {
          totalExecutions: 0,
          successfulExecutions: 0,
          failedExecutions: 0,
          averageExecutionTimeMs: 0,
          lastExecutedAt: undefined,
          lastError: undefined,
          lastErrorAt: undefined
        });
      }
      this.logger.log('Statistics reset for all handlers');
    }
  }

  /**
   * Check if an error is retryable
   */
  private isRetryableError(error: unknown): boolean {
    if (!error) return false;

    const errorMessage = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

    // Network/connection errors are retryable
    const retryableErrors = [
      'connection',
      'timeout',
      'network',
      'temporarily unavailable',
      'service unavailable',
      'rate limit',
      'quota exceeded',
      'database',
      'redis'
    ];

    // Application errors that are not retryable
    const nonRetryableErrors = [
      'validation',
      'invalid',
      'unauthorized',
      'forbidden',
      'not found',
      'bad request',
      'malformed',
      'syntax error'
    ];

    // Check for non-retryable errors first
    if (nonRetryableErrors.some(nonRetryable => errorMessage.includes(nonRetryable))) {
      return false;
    }

    // Check for retryable errors
    if (retryableErrors.some(retryable => errorMessage.includes(retryable))) {
      return true;
    }

    // Default to retryable for unknown errors
    return true;
  }

  /**
   * Validate handler metadata
   */
  private validateHandlerMetadata(metadata: EventHandlerMetadata): void {
    if (!metadata.eventType || metadata.eventType.trim() === '') {
      throw new Error('Handler metadata must have a valid eventType');
    }

    if (!metadata.handler || typeof metadata.handler !== 'function') {
      throw new Error('Handler metadata must have a valid handler function');
    }

    if (!metadata.target) {
      throw new Error('Handler metadata must have a target instance');
    }

    if (!metadata.methodName || metadata.methodName.trim() === '') {
      throw new Error('Handler metadata must have a valid methodName');
    }

    // Validate priority if provided
    if (metadata.options?.priority !== undefined) {
      if (typeof metadata.options.priority !== 'number' || metadata.options.priority < 0) {
        throw new Error('Handler priority must be a non-negative number');
      }
    }
  }

  /**
   * Get handler name for logging and identification
   */
  private getHandlerName(handler: EventHandlerMetadata): string {
    return `${handler.target.constructor.name}.${handler.methodName}`;
  }

  /**
   * Get unique handler key for statistics
   */
  private getHandlerKey(handler: EventHandlerMetadata): string {
    return `${handler.target.constructor.name}.${handler.methodName}`;
  }

  /**
   * Get detailed registry information for debugging
   */
  getRegistryInfo(): {
    totalEventTypes: number;
    totalHandlers: number;
    eventTypes: Array<{
      eventType: string;
      handlerCount: number;
      handlers: Array<{
        className: string;
        methodName: string;
        priority: number;
        parallel: boolean;
      }>;
    }>;
  } {
    const eventTypes: Array<{
      eventType: string;
      handlerCount: number;
      handlers: Array<{
        className: string;
        methodName: string;
        priority: number;
        parallel: boolean;
      }>;
    }> = [];

    for (const [eventType, handlers] of this.eventHandlers.entries()) {
      eventTypes.push({
        eventType,
        handlerCount: handlers.length,
        handlers: handlers.map(h => ({
          className: h.target.constructor.name,
          methodName: h.methodName,
          priority: h.options?.priority || 0,
          parallel: h.options?.parallel !== false
        }))
      });
    }

    return {
      totalEventTypes: this.eventHandlers.size,
      totalHandlers: this.getTotalHandlerCount(),
      eventTypes
    };
  }

  /**
   * Clear all registered handlers (useful for testing)
   */
  clearAll(): void {
    this.eventHandlers.clear();
    this.handlerStats.clear();
    this.logger.log('All handlers cleared from registry');
  }
}
