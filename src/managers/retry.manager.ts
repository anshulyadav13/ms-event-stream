import { Injectable, Logger } from '@nestjs/common';
import { ResolvedEventStreamConfig } from '../interfaces';

/**
 * Error classification types
 */
export enum ErrorType {
  /** Temporary network/connection errors */
  NETWORK = 'network',
  /** Service temporarily unavailable */
  SERVICE_UNAVAILABLE = 'service_unavailable',
  /** Rate limiting */
  RATE_LIMIT = 'rate_limit',
  /** Database connection/timeout errors */
  DATABASE = 'database',
  /** Application logic errors (not retryable) */
  APPLICATION = 'application',
  /** Validation errors (not retryable) */
  VALIDATION = 'validation',
  /** Authorization errors (not retryable) */
  AUTHORIZATION = 'authorization',
  /** Unknown errors (default to retryable) */
  UNKNOWN = 'unknown'
}

/**
 * Retry policy configuration
 */
export interface RetryPolicy {
  /** Maximum number of retry attempts */
  maxAttempts: number;
  /** Base delay in milliseconds for exponential backoff */
  baseDelayMs: number;
  /** Maximum delay in milliseconds */
  maxDelayMs: number;
  /** Multiplier for exponential backoff */
  backoffMultiplier: number;
  /** Whether to add jitter to prevent thundering herd */
  enableJitter: boolean;
  /** Maximum jitter percentage (0-1) */
  jitterPercent: number;
  /** Whether this error type is retryable */
  retryable: boolean;
}

/**
 * Retry attempt result
 */
export interface RetryAttemptResult {
  /** Whether the operation should be retried */
  shouldRetry: boolean;
  /** Delay before next retry in milliseconds */
  delayMs?: number | undefined;
  /** Current attempt number */
  attempt: number;
  /** Maximum attempts allowed */
  maxAttempts: number;
  /** Error classification */
  errorType: ErrorType;
  /** Whether max attempts reached */
  maxAttemptsReached: boolean;
}

/**
 * Retry operation result
 */
export interface RetryOperationResult<T = any> {
  /** Whether the operation succeeded */
  success: boolean;
  /** Result data if successful */
  data?: T;
  /** Error information if failed */
  error?: {
    message: string;
    type: ErrorType;
    attempt: number;
    totalAttempts: number;
    finalError: boolean;
  };
}

/**
 * Circuit breaker state
 */
export enum CircuitBreakerState {
  CLOSED = 'closed',
  OPEN = 'open',
  HALF_OPEN = 'half_open'
}

/**
 * Circuit breaker configuration
 */
export interface CircuitBreakerConfig {
  /** Failure threshold to open circuit */
  failureThreshold: number;
  /** Recovery timeout in milliseconds */
  recoveryTimeoutMs: number;
  /** Time window for counting failures */
  timeWindowMs: number;
  /** Minimum requests before circuit can open */
  minimumRequestThreshold: number;
}

/**
 * Circuit breaker state info
 */
interface CircuitBreakerStateInfo {
  state: CircuitBreakerState;
  failureCount: number;
  lastFailureTime: number;
  lastSuccessTime: number;
  requestCount: number;
  windowStartTime: number;
}

/**
 * Default retry policies for different error types
 */
const DEFAULT_RETRY_POLICIES: Record<ErrorType, RetryPolicy> = {
  [ErrorType.NETWORK]: {
    maxAttempts: 3,
    baseDelayMs: 1000,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
    enableJitter: true,
    jitterPercent: 0.1,
    retryable: true
  },
  [ErrorType.SERVICE_UNAVAILABLE]: {
    maxAttempts: 3,
    baseDelayMs: 2000,
    maxDelayMs: 60000,
    backoffMultiplier: 2,
    enableJitter: true,
    jitterPercent: 0.15,
    retryable: true
  },
  [ErrorType.RATE_LIMIT]: {
    maxAttempts: 5,
    baseDelayMs: 5000,
    maxDelayMs: 120000,
    backoffMultiplier: 1.5,
    enableJitter: true,
    jitterPercent: 0.2,
    retryable: true
  },
  [ErrorType.DATABASE]: {
    maxAttempts: 3,
    baseDelayMs: 1500,
    maxDelayMs: 30000,
    backoffMultiplier: 2,
    enableJitter: true,
    jitterPercent: 0.1,
    retryable: true
  },
  [ErrorType.APPLICATION]: {
    maxAttempts: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    backoffMultiplier: 1,
    enableJitter: false,
    jitterPercent: 0,
    retryable: false
  },
  [ErrorType.VALIDATION]: {
    maxAttempts: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    backoffMultiplier: 1,
    enableJitter: false,
    jitterPercent: 0,
    retryable: false
  },
  [ErrorType.AUTHORIZATION]: {
    maxAttempts: 0,
    baseDelayMs: 0,
    maxDelayMs: 0,
    backoffMultiplier: 1,
    enableJitter: false,
    jitterPercent: 0,
    retryable: false
  },
  [ErrorType.UNKNOWN]: {
    maxAttempts: 2,
    baseDelayMs: 1000,
    maxDelayMs: 10000,
    backoffMultiplier: 2,
    enableJitter: true,
    jitterPercent: 0.1,
    retryable: true
  }
};

/**
 * Default circuit breaker configuration
 */
const DEFAULT_CIRCUIT_BREAKER_CONFIG: CircuitBreakerConfig = {
  failureThreshold: 5,
  recoveryTimeoutMs: 60000,
  timeWindowMs: 60000,
  minimumRequestThreshold: 10
};

/**
 * Retry manager handles retry logic with exponential backoff and circuit breaker pattern
 */
@Injectable()
export class RetryManager {
  private readonly logger = new Logger(RetryManager.name);
  private readonly retryPolicies: Map<ErrorType, RetryPolicy> = new Map();
  private readonly circuitBreakers: Map<string, CircuitBreakerStateInfo> = new Map();
  private readonly circuitBreakerConfig: CircuitBreakerConfig;

  constructor(private readonly config: ResolvedEventStreamConfig) {
    this.initializeRetryPolicies();
    this.circuitBreakerConfig = this.getCircuitBreakerConfig();
    this.logger.log('RetryManager initialized with exponential backoff and circuit breaker');
  }

  /**
   * Initialize retry policies from configuration
   */
  private initializeRetryPolicies(): void {
    // Start with default policies
    Object.entries(DEFAULT_RETRY_POLICIES).forEach(([errorType, policy]) => {
      this.retryPolicies.set(errorType as ErrorType, { ...policy });
    });

    // Override with configuration if provided
    if (this.config.outbox?.maxRetries) {
      const maxRetries = this.config.outbox.maxRetries;
      this.retryPolicies.forEach((policy, errorType) => {
        if (policy.retryable) {
          policy.maxAttempts = maxRetries;
        }
      });
    }

    if (this.config.inbox?.maxRetries) {
      const maxRetries = this.config.inbox.maxRetries;
      this.retryPolicies.forEach((policy, errorType) => {
        if (policy.retryable) {
          policy.maxAttempts = Math.max(policy.maxAttempts, maxRetries);
        }
      });
    }

    this.logger.debug('Retry policies initialized', {
      policies: Object.fromEntries(this.retryPolicies)
    });
  }

  /**
   * Get circuit breaker configuration
   */
  private getCircuitBreakerConfig(): CircuitBreakerConfig {
    // Use default config for now, can be extended with configuration
    return { ...DEFAULT_CIRCUIT_BREAKER_CONFIG };
  }

  /**
   * Classify error type based on error message/instance
   */
  classifyError(error: unknown): ErrorType {
    if (!error) {
      return ErrorType.UNKNOWN;
    }

    const errorMessage = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

    // Network errors
    if (this.containsAny(errorMessage, ['connection', 'timeout', 'network', 'econnrefused', 'enotfound', 'etimedout'])) {
      return ErrorType.NETWORK;
    }

    // Service unavailable
    if (this.containsAny(errorMessage, ['service unavailable', 'temporarily unavailable', '503', 'server error', '502', '504'])) {
      return ErrorType.SERVICE_UNAVAILABLE;
    }

    // Rate limiting
    if (this.containsAny(errorMessage, ['rate limit', 'quota exceeded', 'too many requests', '429', 'throttle'])) {
      return ErrorType.RATE_LIMIT;
    }

    // Database errors
    if (this.containsAny(errorMessage, ['database', 'connection pool', 'deadlock', 'lock timeout', 'constraint violation'])) {
      return ErrorType.DATABASE;
    }

    // Validation errors
    if (this.containsAny(errorMessage, ['validation', 'invalid', 'malformed', 'bad request', '400', 'schema'])) {
      return ErrorType.VALIDATION;
    }

    // Authorization errors
    if (this.containsAny(errorMessage, ['unauthorized', 'forbidden', '401', '403', 'access denied', 'permission'])) {
      return ErrorType.AUTHORIZATION;
    }

    // Application errors
    if (this.containsAny(errorMessage, ['not found', '404', 'conflict', '409', 'precondition', 'business rule'])) {
      return ErrorType.APPLICATION;
    }

    return ErrorType.UNKNOWN;
  }

  /**
   * Check if a string contains any of the given substrings
   */
  private containsAny(str: string, substrings: string[]): boolean {
    return substrings.some(substring => str.includes(substring));
  }

  /**
   * Determine if operation should be retried and calculate delay
   */
  shouldRetry(error: unknown, attempt: number, customPolicy?: Partial<RetryPolicy>): RetryAttemptResult {
    const errorType = this.classifyError(error);
    const policy = this.getEffectivePolicy(errorType, customPolicy);

    const maxAttemptsReached = attempt >= policy.maxAttempts;
    const shouldRetry = policy.retryable && !maxAttemptsReached;

    let delayMs: number | undefined;
    if (shouldRetry) {
      delayMs = this.calculateDelay(attempt, policy);
    }

    const result: RetryAttemptResult = {
      shouldRetry,
      delayMs,
      attempt,
      maxAttempts: policy.maxAttempts,
      errorType,
      maxAttemptsReached
    };

    this.logger.debug('Retry decision', {
      errorType,
      attempt,
      maxAttempts: policy.maxAttempts,
      shouldRetry,
      delayMs,
      errorMessage: error instanceof Error ? error.message : String(error)
    });

    return result;
  }

  /**
   * Get effective retry policy combining default and custom policies
   */
  private getEffectivePolicy(errorType: ErrorType, customPolicy?: Partial<RetryPolicy>): RetryPolicy {
    const defaultPolicy = this.retryPolicies.get(errorType) || this.retryPolicies.get(ErrorType.UNKNOWN)!;
    
    if (!customPolicy) {
      return defaultPolicy;
    }

    return {
      ...defaultPolicy,
      ...customPolicy
    };
  }

  /**
   * Calculate delay with exponential backoff and optional jitter
   */
  calculateDelay(attempt: number, policy: RetryPolicy): number {
    // Exponential backoff: baseDelay * (multiplier ^ (attempt - 1))
    let delay = policy.baseDelayMs * Math.pow(policy.backoffMultiplier, attempt - 1);
    
    // Cap at maximum delay
    delay = Math.min(delay, policy.maxDelayMs);

    // Add jitter if enabled
    if (policy.enableJitter && policy.jitterPercent > 0) {
      const jitterAmount = delay * policy.jitterPercent;
      const jitter = (Math.random() - 0.5) * 2 * jitterAmount;
      delay = Math.max(0, delay + jitter);
    }

    return Math.floor(delay);
  }

  /**
   * Execute operation with retry logic
   */
  async executeWithRetry<T>(
    operation: () => Promise<T>,
    operationName: string,
    customPolicy?: Partial<RetryPolicy>
  ): Promise<RetryOperationResult<T>> {
    let attempt = 0;
    let lastError: unknown;

    // Check circuit breaker before starting
    if (this.isCircuitOpen(operationName)) {
      this.logger.warn(`Circuit breaker is open for operation: ${operationName}`);
      return {
        success: false,
        error: {
          message: 'Circuit breaker is open',
          type: ErrorType.SERVICE_UNAVAILABLE,
          attempt: 0,
          totalAttempts: 0,
          finalError: true
        }
      };
    }

    while (true) {
      attempt++;
      
      try {
        this.logger.debug(`Executing operation attempt ${attempt}: ${operationName}`);
        
        const result = await operation();
        
        // Operation succeeded, record success for circuit breaker
        this.recordCircuitBreakerSuccess(operationName);
        
        this.logger.debug(`Operation succeeded on attempt ${attempt}: ${operationName}`);
        
        return {
          success: true,
          data: result
        };

      } catch (error) {
        lastError = error;
        const errorMessage = error instanceof Error ? error.message : String(error);
        
        // Record failure for circuit breaker
        this.recordCircuitBreakerFailure(operationName);
        
        this.logger.warn(`Operation failed on attempt ${attempt}: ${operationName}`, {
          error: errorMessage,
          attempt,
          operationName
        });

        // Determine if we should retry
        const retryDecision = this.shouldRetry(error, attempt, customPolicy);
        
        if (!retryDecision.shouldRetry) {
          this.logger.error(`Operation failed permanently: ${operationName}`, {
            error: errorMessage,
            totalAttempts: attempt,
            errorType: retryDecision.errorType,
            maxAttemptsReached: retryDecision.maxAttemptsReached
          });

          return {
            success: false,
            error: {
              message: errorMessage,
              type: retryDecision.errorType,
              attempt,
              totalAttempts: attempt,
              finalError: true
            }
          };
        }

        // Wait before next retry
        if (retryDecision.delayMs && retryDecision.delayMs > 0) {
          this.logger.debug(`Waiting ${retryDecision.delayMs}ms before retry ${attempt + 1}: ${operationName}`);
          await this.sleep(retryDecision.delayMs);
        }
      }
    }
  }

  /**
   * Sleep for specified milliseconds
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Check if circuit breaker is open for an operation
   */
  private isCircuitOpen(operationName: string): boolean {
    const circuitState = this.getCircuitBreakerState(operationName);
    
    switch (circuitState.state) {
      case CircuitBreakerState.CLOSED:
        return false;
        
      case CircuitBreakerState.OPEN:
        // Check if recovery timeout has passed
        const now = Date.now();
        if (now - circuitState.lastFailureTime >= this.circuitBreakerConfig.recoveryTimeoutMs) {
          // Transition to half-open
          circuitState.state = CircuitBreakerState.HALF_OPEN;
          this.logger.log(`Circuit breaker transitioning to half-open: ${operationName}`);
          return false;
        }
        return true;
        
      case CircuitBreakerState.HALF_OPEN:
        return false;
        
      default:
        return false;
    }
  }

  /**
   * Get or create circuit breaker state for an operation
   */
  private getCircuitBreakerState(operationName: string): CircuitBreakerStateInfo {
    if (!this.circuitBreakers.has(operationName)) {
      this.circuitBreakers.set(operationName, {
        state: CircuitBreakerState.CLOSED,
        failureCount: 0,
        lastFailureTime: 0,
        lastSuccessTime: 0,
        requestCount: 0,
        windowStartTime: Date.now()
      });
    }
    
    return this.circuitBreakers.get(operationName)!;
  }

  /**
   * Record successful operation for circuit breaker
   */
  private recordCircuitBreakerSuccess(operationName: string): void {
    const circuitState = this.getCircuitBreakerState(operationName);
    const now = Date.now();
    
    circuitState.lastSuccessTime = now;
    circuitState.requestCount++;
    
    // Reset window if needed
    this.resetWindowIfNeeded(circuitState, now);
    
    // If half-open, transition back to closed
    if (circuitState.state === CircuitBreakerState.HALF_OPEN) {
      circuitState.state = CircuitBreakerState.CLOSED;
      circuitState.failureCount = 0;
      this.logger.log(`Circuit breaker closed after successful operation: ${operationName}`);
    }
  }

  /**
   * Record failed operation for circuit breaker
   */
  private recordCircuitBreakerFailure(operationName: string): void {
    const circuitState = this.getCircuitBreakerState(operationName);
    const now = Date.now();
    
    circuitState.lastFailureTime = now;
    circuitState.failureCount++;
    circuitState.requestCount++;
    
    // Reset window if needed
    this.resetWindowIfNeeded(circuitState, now);
    
    // Check if we should open the circuit
    const shouldOpen = 
      circuitState.requestCount >= this.circuitBreakerConfig.minimumRequestThreshold &&
      circuitState.failureCount >= this.circuitBreakerConfig.failureThreshold;
    
    if (shouldOpen && circuitState.state !== CircuitBreakerState.OPEN) {
      circuitState.state = CircuitBreakerState.OPEN;
      this.logger.warn(`Circuit breaker opened due to failures: ${operationName}`, {
        failureCount: circuitState.failureCount,
        threshold: this.circuitBreakerConfig.failureThreshold,
        requestCount: circuitState.requestCount
      });
    }
  }

  /**
   * Reset failure window if time window has passed
   */
  private resetWindowIfNeeded(circuitState: CircuitBreakerStateInfo, now: number): void {
    if (now - circuitState.windowStartTime >= this.circuitBreakerConfig.timeWindowMs) {
      circuitState.failureCount = 0;
      circuitState.requestCount = 0;
      circuitState.windowStartTime = now;
    }
  }

  /**
   * Get retry statistics for monitoring
   */
  getRetryStats(): {
    policies: Record<string, RetryPolicy>;
    circuitBreakers: Record<string, {
      state: CircuitBreakerState;
      failureCount: number;
      requestCount: number;
    }>;
  } {
    const policies = Object.fromEntries(this.retryPolicies);
    const circuitBreakers: Record<string, any> = {};
    
    this.circuitBreakers.forEach((state, name) => {
      circuitBreakers[name] = {
        state: state.state,
        failureCount: state.failureCount,
        requestCount: state.requestCount
      };
    });
    
    return {
      policies,
      circuitBreakers
    };
  }

  /**
   * Reset circuit breaker state for an operation
   */
  resetCircuitBreaker(operationName: string): void {
    this.circuitBreakers.delete(operationName);
    this.logger.log(`Circuit breaker reset for operation: ${operationName}`);
  }

  /**
   * Reset all circuit breakers
   */
  resetAllCircuitBreakers(): void {
    this.circuitBreakers.clear();
    this.logger.log('All circuit breakers reset');
  }

  /**
   * Update retry policy for specific error type
   */
  updateRetryPolicy(errorType: ErrorType, policy: Partial<RetryPolicy>): void {
    const currentPolicy = this.retryPolicies.get(errorType) || this.retryPolicies.get(ErrorType.UNKNOWN)!;
    const updatedPolicy = { ...currentPolicy, ...policy };
    
    this.retryPolicies.set(errorType, updatedPolicy);
    
    this.logger.log(`Retry policy updated for error type: ${errorType}`, {
      updatedPolicy
    });
  }

  /**
   * Get retry policy for error type
   */
  getRetryPolicy(errorType: ErrorType): RetryPolicy | undefined {
    return this.retryPolicies.get(errorType);
  }
}
