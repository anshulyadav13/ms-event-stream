import { Injectable, Logger, OnModuleInit, Type, Inject } from '@nestjs/common';
import { DiscoveryService, ModuleRef, Reflector } from '@nestjs/core';
import { InstanceWrapper } from '@nestjs/core/injector/instance-wrapper';
import { EventStreamService } from '../event-stream.service';
import { EventHandlerMetadata, EventHandler } from '../interfaces/event.interface';
import { ResolvedEventStreamConfig } from '../interfaces/config.interface';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';
import { 
  EVENT_HANDLER_METADATA, 
  EventHandlerDecoratorMetadata,
  getEventHandlerMetadata,
  OnEventOptions 
} from '../decorators/on-event.decorator';

/**
 * Discovery statistics for monitoring
 */
export interface DiscoveryStats {
  classesScanned: number;
  handlersFound: number;
  handlersRegistered: number;
  handlersFailed: number;
  eventTypesDiscovered: string[];
  discoveryTimeMs: number;
  errors: Array<{
    className: string;
    methodName: string;
    error: string;
  }>;
}

/**
 * Event handler discovery configuration
 */
export interface HandlerDiscoveryConfig {
  /**
   * Whether to enable automatic discovery
   * @default true
   */
  enabled?: boolean;

  /**
   * Packages/modules to include in discovery
   * If empty, scans all modules
   */
  includeModules?: string[];

  /**
   * Packages/modules to exclude from discovery
   */
  excludeModules?: string[];

  /**
   * Whether to validate handler methods before registration
   * @default true
   */
  validateHandlers?: boolean;

  /**
   * Whether to throw errors on discovery failures
   * @default false (log errors but continue)
   */
  throwOnError?: boolean;

  /**
   * Whether to log discovery process details
   * @default true
   */
  enableLogging?: boolean;
}

/**
 * Service for automatically discovering and registering event handlers
 * decorated with @OnEvent
 * 
 * This service scans all providers in the NestJS module tree to find
 * methods decorated with @OnEvent and automatically registers them
 * with the EventStreamService.
 */
@Injectable()
export class EventHandlerDiscovery implements OnModuleInit {
  private readonly logger = new Logger(EventHandlerDiscovery.name);
  private readonly config: HandlerDiscoveryConfig;
  private isDiscoveryComplete = false;
  private discoveryStats: DiscoveryStats = {
    classesScanned: 0,
    handlersFound: 0,
    handlersRegistered: 0,
    handlersFailed: 0,
    eventTypesDiscovered: [],
    discoveryTimeMs: 0,
    errors: []
  };

  constructor(
    private readonly discoveryService: DiscoveryService,
    private readonly moduleRef: ModuleRef,
    private readonly reflector: Reflector,
    private readonly eventStreamService: EventStreamService,
    @Inject(EVENT_STREAM_CONFIG) private readonly fullConfig: ResolvedEventStreamConfig
  ) {
    // Extract discovery configuration with defaults
    this.config = {
      ...fullConfig.discovery,
      enabled: fullConfig.discovery.enabled ?? true,
      validateHandlers: fullConfig.discovery.validateHandlers ?? true,
      throwOnError: fullConfig.discovery.throwOnError ?? false,
      enableLogging: fullConfig.discovery.enableLogging ?? true,
    } as HandlerDiscoveryConfig;
  }

  /**
   * Initialize handler discovery when the module starts
   */
  async onModuleInit(): Promise<void> {
    if (!this.config.enabled) {
      this.logger.log('Event handler discovery is disabled');
      return;
    }

    this.logger.log('Starting automatic event handler discovery...');
    
    try {
      await this.discoverAndRegisterHandlers();
      this.isDiscoveryComplete = true;
      
      if (this.config.enableLogging) {
        this.logDiscoveryResults();
      }
    } catch (error) {
      this.logger.error('Event handler discovery failed', error);
      
      if (this.config.throwOnError) {
        throw error;
      }
    }
  }

  /**
   * Discover and register all event handlers
   */
  async discoverAndRegisterHandlers(): Promise<void> {
    const startTime = Date.now();
    this.resetStats();

    // Get all providers from all modules
    const providers = this.discoveryService.getProviders();
    
    if (this.config.enableLogging) {
      this.logger.debug(`Scanning ${providers.length} providers for event handlers...`);
    }

    // Process each provider
    for (const wrapper of providers) {
      try {
        await this.processProvider(wrapper);
      } catch (error) {
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error(`Failed to process provider: ${this.getProviderName(wrapper)}`, error);
        
        this.discoveryStats.errors.push({
          className: this.getProviderName(wrapper),
          methodName: 'N/A',
          error: errorMessage
        });

        if (this.config.throwOnError) {
          throw error;
        }
      }
    }

    this.discoveryStats.discoveryTimeMs = Date.now() - startTime;
    this.discoveryStats.eventTypesDiscovered = [...new Set(this.discoveryStats.eventTypesDiscovered)];
  }

  /**
   * Process a single provider to find and register event handlers
   */
  private async processProvider(wrapper: InstanceWrapper): Promise<void> {
    // Skip if no metatype or instance
    if (!wrapper.metatype || !wrapper.instance) {
      return;
    }

    const providerName = this.getProviderName(wrapper);
    
    // Skip if module should be excluded
    if (this.shouldExcludeProvider(wrapper)) {
      if (this.config.enableLogging) {
        this.logger.debug(`Skipping excluded provider: ${providerName}`);
      }
      return;
    }

    this.discoveryStats.classesScanned++;

    // Get event handler metadata from the class
    const handlerMetadata = getEventHandlerMetadata(wrapper.metatype);
    
    if (handlerMetadata.length === 0) {
      return; // No event handlers in this class
    }

    if (this.config.enableLogging) {
      this.logger.debug(`Found ${handlerMetadata.length} event handler(s) in ${providerName}`);
    }

    // Register each handler
    for (const metadata of handlerMetadata) {
      try {
        await this.registerHandler(wrapper, metadata);
        this.discoveryStats.handlersRegistered++;
        
        if (!this.discoveryStats.eventTypesDiscovered.includes(metadata.eventType)) {
          this.discoveryStats.eventTypesDiscovered.push(metadata.eventType);
        }
      } catch (error) {
        this.discoveryStats.handlersFailed++;
        
        const errorMessage = error instanceof Error ? error.message : String(error);
        this.logger.error(`Failed to register handler: ${providerName}.${metadata.methodName}`, error);
        
        this.discoveryStats.errors.push({
          className: providerName,
          methodName: metadata.methodName,
          error: errorMessage
        });

        if (this.config.throwOnError) {
          throw error;
        }
      }
    }

    this.discoveryStats.handlersFound += handlerMetadata.length;
  }

  /**
   * Register a single event handler
   */
  private async registerHandler(
    wrapper: InstanceWrapper, 
    metadata: EventHandlerDecoratorMetadata
  ): Promise<void> {
    const instance = wrapper.instance;
    const methodName = metadata.methodName;
    const eventType = metadata.eventType;
    const options = metadata.options;

    // Validate the handler method
    if (this.config.validateHandlers) {
      this.validateHandler(instance, methodName, eventType);
    }

    // Get the handler function
    const handlerFunction = instance[methodName];
    if (typeof handlerFunction !== 'function') {
      throw new Error(`Handler method '${methodName}' is not a function`);
    }

    // Create EventHandlerMetadata for registration
    const eventHandlerMetadata: EventHandlerMetadata = {
      eventType,
      handler: this.createHandlerWrapper(instance, handlerFunction, options),
      target: instance,
      methodName,
      options: this.convertToHandlerOptions(options)
    };

    // Register with EventStreamService
    this.eventStreamService.registerEventHandler(eventHandlerMetadata);

    if (this.config.enableLogging) {
      this.logger.debug(
        `Registered handler: ${this.getProviderName(wrapper)}.${methodName} -> '${eventType}' (priority: ${options.priority || 0})`
      );
    }
  }

  /**
   * Create a wrapper function for the handler that includes additional features
   */
  private createHandlerWrapper(
    instance: any, 
    originalHandler: Function, 
    options: OnEventOptions
  ): EventHandler {
    return async (eventPayload: any) => {
      // Apply event filter if configured
      if (options.filter && !options.filter(eventPayload)) {
        if (this.config.enableLogging) {
          this.logger.debug(`Event filtered out by handler filter: ${eventPayload.metadata.eventType} (${eventPayload.metadata.eventId})`);
        }
        return;
      }

      // Apply timeout if configured
      if (options.timeoutMs && options.timeoutMs > 0) {
        return await this.executeWithTimeout(
          () => originalHandler.call(instance, eventPayload),
          options.timeoutMs
        );
      }

      // Execute the original handler
      return await originalHandler.call(instance, eventPayload);
    };
  }

  /**
   * Execute a function with a timeout
   */
  private async executeWithTimeout<T>(
    fn: () => Promise<T>, 
    timeoutMs: number
  ): Promise<T> {
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`Handler execution timeout after ${timeoutMs}ms`));
      }, timeoutMs);

      fn()
        .then(result => {
          clearTimeout(timer);
          resolve(result);
        })
        .catch(error => {
          clearTimeout(timer);
          reject(error);
        });
    });
  }

  /**
   * Convert OnEventOptions to EventHandlerMetadata options
   */
  private convertToHandlerOptions(options: OnEventOptions): EventHandlerMetadata['options'] {
    return {
      priority: options.priority,
      parallel: options.parallel,
      retryPolicy: options.retryPolicy
    };
  }

  /**
   * Validate a handler method
   */
  private validateHandler(instance: any, methodName: string, eventType: string): void {
    if (!instance) {
      throw new Error(`Handler instance is null or undefined for event type '${eventType}'`);
    }

    const method = instance[methodName];
    if (!method) {
      throw new Error(`Handler method '${methodName}' does not exist on instance for event type '${eventType}'`);
    }

    if (typeof method !== 'function') {
      throw new Error(`Handler '${methodName}' is not a function for event type '${eventType}'`);
    }

    // Check if method is async or returns a Promise
    const isAsync = method.constructor.name === 'AsyncFunction';
    if (!isAsync) {
      // Test with a dummy call to see if it returns a Promise
      try {
        const dummyEvent = {
          metadata: { eventId: 'test', eventType: 'test', createdAt: new Date() },
          data: {}
        };
        const result = method.call(instance, dummyEvent);
        if (result && typeof result.then === 'function') {
          // It's a Promise, which is good
        } else if (result !== undefined) {
          this.logger.warn(
            `Handler '${methodName}' for event '${eventType}' should be async or return a Promise`
          );
        }
      } catch (error) {
        // Ignore errors during validation call
      }
    }
  }

  /**
   * Check if a provider should be excluded from discovery
   */
  private shouldExcludeProvider(wrapper: InstanceWrapper): boolean {
    const providerName = this.getProviderName(wrapper);
    
    // Check exclude list
    if (this.config.excludeModules) {
      for (const excludePattern of this.config.excludeModules) {
        if (providerName.includes(excludePattern)) {
          return true;
        }
      }
    }

    // Check include list (if specified, only include matching)
    if (this.config.includeModules && this.config.includeModules.length > 0) {
      let shouldInclude = false;
      for (const includePattern of this.config.includeModules) {
        if (providerName.includes(includePattern)) {
          shouldInclude = true;
          break;
        }
      }
      return !shouldInclude;
    }

    return false;
  }

  /**
   * Get a readable name for a provider
   */
  private getProviderName(wrapper: InstanceWrapper): string {
    if (wrapper.metatype?.name) {
      return wrapper.metatype.name;
    }
    if (wrapper.name) {
      return wrapper.name.toString();
    }
    return 'UnknownProvider';
  }

  /**
   * Reset discovery statistics
   */
  private resetStats(): void {
    this.discoveryStats = {
      classesScanned: 0,
      handlersFound: 0,
      handlersRegistered: 0,
      handlersFailed: 0,
      eventTypesDiscovered: [],
      discoveryTimeMs: 0,
      errors: []
    };
  }

  /**
   * Log discovery results
   */
  private logDiscoveryResults(): void {
    const stats = this.discoveryStats;
    
    this.logger.log(`Event handler discovery completed in ${stats.discoveryTimeMs}ms`);
    this.logger.log(`Scanned ${stats.classesScanned} classes, found ${stats.handlersFound} handlers`);
    this.logger.log(`Registered ${stats.handlersRegistered} handlers, ${stats.handlersFailed} failed`);
    
    if (stats.eventTypesDiscovered.length > 0) {
      this.logger.log(`Event types discovered: ${stats.eventTypesDiscovered.join(', ')}`);
    }

    if (stats.errors.length > 0) {
      this.logger.warn(`Discovery errors: ${stats.errors.length}`);
      stats.errors.forEach(error => {
        this.logger.warn(`  ${error.className}.${error.methodName}: ${error.error}`);
      });
    }
  }

  /**
   * Get discovery statistics
   */
  getDiscoveryStats(): DiscoveryStats {
    return { ...this.discoveryStats };
  }

  /**
   * Check if discovery is complete
   */
  isDiscoveryCompleted(): boolean {
    return this.isDiscoveryComplete;
  }

  /**
   * Manually trigger handler discovery (useful for testing or dynamic scenarios)
   */
  async rediscoverHandlers(): Promise<void> {
    this.logger.log('Manually triggering event handler rediscovery...');
    this.isDiscoveryComplete = false;
    await this.discoverAndRegisterHandlers();
    this.isDiscoveryComplete = true;
    
    if (this.config.enableLogging) {
      this.logDiscoveryResults();
    }
  }

  /**
   * Get all discovered event types
   */
  getDiscoveredEventTypes(): string[] {
    return [...this.discoveryStats.eventTypesDiscovered];
  }

  /**
   * Get discovery errors
   */
  getDiscoveryErrors(): Array<{ className: string; methodName: string; error: string }> {
    return [...this.discoveryStats.errors];
  }

  /**
   * Clear discovery errors (useful for monitoring systems)
   */
  clearDiscoveryErrors(): void {
    this.discoveryStats.errors = [];
  }
}
