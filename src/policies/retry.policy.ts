import { ErrorType, RetryPolicy } from '../managers/retry.manager';

/**
 * Retry policy builder for fluent configuration
 */
export class RetryPolicyBuilder {
  private policy: Partial<RetryPolicy> = {};

  /**
   * Set maximum number of retry attempts
   */
  maxAttempts(attempts: number): RetryPolicyBuilder {
    this.policy.maxAttempts = attempts;
    return this;
  }

  /**
   * Set base delay for exponential backoff
   */
  baseDelay(delayMs: number): RetryPolicyBuilder {
    this.policy.baseDelayMs = delayMs;
    return this;
  }

  /**
   * Set maximum delay cap
   */
  maxDelay(delayMs: number): RetryPolicyBuilder {
    this.policy.maxDelayMs = delayMs;
    return this;
  }

  /**
   * Set backoff multiplier for exponential growth
   */
  backoffMultiplier(multiplier: number): RetryPolicyBuilder {
    this.policy.backoffMultiplier = multiplier;
    return this;
  }

  /**
   * Enable or disable jitter
   */
  jitter(enabled: boolean, percent?: number): RetryPolicyBuilder {
    this.policy.enableJitter = enabled;
    if (percent !== undefined) {
      this.policy.jitterPercent = Math.max(0, Math.min(1, percent));
    }
    return this;
  }

  /**
   * Set whether this policy allows retries
   */
  retryable(retryable: boolean): RetryPolicyBuilder {
    this.policy.retryable = retryable;
    return this;
  }

  /**
   * Build the final retry policy
   */
  build(): Partial<RetryPolicy> {
    return { ...this.policy };
  }
}

/**
 * Predefined retry policies for common scenarios
 */
export class RetryPolicies {
  /**
   * No retry policy - fail immediately
   */
  static none(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(0)
      .retryable(false)
      .build();
  }

  /**
   * Quick retry policy for fast operations
   */
  static quick(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(2)
      .baseDelay(100)
      .maxDelay(1000)
      .backoffMultiplier(2)
      .jitter(true, 0.1)
      .retryable(true)
      .build();
  }

  /**
   * Standard retry policy for most operations
   */
  static standard(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(3)
      .baseDelay(1000)
      .maxDelay(30000)
      .backoffMultiplier(2)
      .jitter(true, 0.1)
      .retryable(true)
      .build();
  }

  /**
   * Aggressive retry policy for critical operations
   */
  static aggressive(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(5)
      .baseDelay(500)
      .maxDelay(60000)
      .backoffMultiplier(1.5)
      .jitter(true, 0.15)
      .retryable(true)
      .build();
  }

  /**
   * Conservative retry policy for expensive operations
   */
  static conservative(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(2)
      .baseDelay(2000)
      .maxDelay(30000)
      .backoffMultiplier(3)
      .jitter(true, 0.2)
      .retryable(true)
      .build();
  }

  /**
   * Network-specific retry policy
   */
  static network(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(4)
      .baseDelay(1000)
      .maxDelay(30000)
      .backoffMultiplier(2)
      .jitter(true, 0.1)
      .retryable(true)
      .build();
  }

  /**
   * Database-specific retry policy
   */
  static database(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(3)
      .baseDelay(1500)
      .maxDelay(30000)
      .backoffMultiplier(2)
      .jitter(true, 0.1)
      .retryable(true)
      .build();
  }

  /**
   * Rate limit specific retry policy with longer delays
   */
  static rateLimit(): Partial<RetryPolicy> {
    return new RetryPolicyBuilder()
      .maxAttempts(5)
      .baseDelay(5000)
      .maxDelay(120000)
      .backoffMultiplier(1.5)
      .jitter(true, 0.2)
      .retryable(true)
      .build();
  }

  /**
   * Custom retry policy builder
   */
  static custom(): RetryPolicyBuilder {
    return new RetryPolicyBuilder();
  }
}

/**
 * Error condition interfaces for retry logic
 */
export interface RetryCondition {
  /**
   * Check if the error should trigger a retry
   */
  shouldRetry(error: unknown, attempt: number): boolean;
}

/**
 * Error message based retry condition
 */
export class MessageBasedRetryCondition implements RetryCondition {
  constructor(
    private readonly retryableMessages: string[],
    private readonly nonRetryableMessages: string[] = []
  ) {}

  shouldRetry(error: unknown, attempt: number): boolean {
    const errorMessage = error instanceof Error ? error.message.toLowerCase() : String(error).toLowerCase();

    // Check for explicitly non-retryable messages first
    if (this.nonRetryableMessages.some(msg => errorMessage.includes(msg.toLowerCase()))) {
      return false;
    }

    // Check for retryable messages
    return this.retryableMessages.some(msg => errorMessage.includes(msg.toLowerCase()));
  }
}

/**
 * Error type based retry condition
 */
export class TypeBasedRetryCondition implements RetryCondition {
  constructor(
    private readonly retryableTypes: (new (...args: any[]) => Error)[],
    private readonly nonRetryableTypes: (new (...args: any[]) => Error)[] = []
  ) {}

  shouldRetry(error: unknown, attempt: number): boolean {
    if (!(error instanceof Error)) {
      return true; // Default to retryable for non-Error objects
    }

    // Check for explicitly non-retryable types first
    if (this.nonRetryableTypes.some(type => error instanceof type)) {
      return false;
    }

    // Check for retryable types
    return this.retryableTypes.some(type => error instanceof type);
  }
}

/**
 * Composite retry condition that combines multiple conditions
 */
export class CompositeRetryCondition implements RetryCondition {
  constructor(
    private readonly conditions: RetryCondition[],
    private readonly operator: 'AND' | 'OR' = 'OR'
  ) {}

  shouldRetry(error: unknown, attempt: number): boolean {
    if (this.conditions.length === 0) {
      return true;
    }

    if (this.operator === 'AND') {
      return this.conditions.every(condition => condition.shouldRetry(error, attempt));
    } else {
      return this.conditions.some(condition => condition.shouldRetry(error, attempt));
    }
  }
}

/**
 * Attempt-based retry condition
 */
export class AttemptBasedRetryCondition implements RetryCondition {
  constructor(
    private readonly maxAttempts: number,
    private readonly baseCondition?: RetryCondition
  ) {}

  shouldRetry(error: unknown, attempt: number): boolean {
    if (attempt >= this.maxAttempts) {
      return false;
    }

    if (this.baseCondition) {
      return this.baseCondition.shouldRetry(error, attempt);
    }

    return true;
  }
}

/**
 * Time-based retry condition
 */
export class TimeBasedRetryCondition implements RetryCondition {
  private readonly startTime: number = Date.now();

  constructor(
    private readonly timeoutMs: number,
    private readonly baseCondition?: RetryCondition
  ) {}

  shouldRetry(error: unknown, attempt: number): boolean {
    if (Date.now() - this.startTime >= this.timeoutMs) {
      return false;
    }

    if (this.baseCondition) {
      return this.baseCondition.shouldRetry(error, attempt);
    }

    return true;
  }
}

/**
 * Predefined retry conditions for common scenarios
 */
export class RetryConditions {
  /**
   * Network-related error conditions
   */
  static network(): RetryCondition {
    return new MessageBasedRetryCondition(
      ['connection', 'timeout', 'network', 'econnrefused', 'enotfound', 'etimedout'],
      ['unauthorized', 'forbidden', 'not found', 'validation', 'malformed']
    );
  }

  /**
   * Database-related error conditions
   */
  static database(): RetryCondition {
    return new MessageBasedRetryCondition(
      ['connection pool', 'deadlock', 'lock timeout', 'database timeout'],
      ['constraint violation', 'foreign key', 'unique constraint', 'check constraint']
    );
  }

  /**
   * Service availability conditions
   */
  static serviceAvailability(): RetryCondition {
    return new MessageBasedRetryCondition(
      ['service unavailable', 'temporarily unavailable', '503', '502', '504'],
      ['not found', '404', 'unauthorized', '401', 'forbidden', '403']
    );
  }

  /**
   * Rate limiting conditions
   */
  static rateLimit(): RetryCondition {
    return new MessageBasedRetryCondition(
      ['rate limit', 'quota exceeded', 'too many requests', '429', 'throttle']
    );
  }

  /**
   * Never retry condition
   */
  static never(): RetryCondition {
    return {
      shouldRetry: () => false
    };
  }

  /**
   * Always retry condition (within attempt limits)
   */
  static always(maxAttempts: number = 3): RetryCondition {
    return new AttemptBasedRetryCondition(maxAttempts);
  }

  /**
   * Custom condition builder
   */
  static custom(): {
    messages: (retryable: string[], nonRetryable?: string[]) => RetryCondition;
    types: (retryable: (new (...args: any[]) => Error)[], nonRetryable?: (new (...args: any[]) => Error)[]) => RetryCondition;
    attempts: (maxAttempts: number, baseCondition?: RetryCondition) => RetryCondition;
    timeout: (timeoutMs: number, baseCondition?: RetryCondition) => RetryCondition;
    composite: (conditions: RetryCondition[], operator?: 'AND' | 'OR') => RetryCondition;
  } {
    return {
      messages: (retryable, nonRetryable) => new MessageBasedRetryCondition(retryable, nonRetryable),
      types: (retryable, nonRetryable) => new TypeBasedRetryCondition(retryable, nonRetryable),
      attempts: (maxAttempts, baseCondition) => new AttemptBasedRetryCondition(maxAttempts, baseCondition),
      timeout: (timeoutMs, baseCondition) => new TimeBasedRetryCondition(timeoutMs, baseCondition),
      composite: (conditions, operator) => new CompositeRetryCondition(conditions, operator)
    };
  }
}

/**
 * Retry context information
 */
export interface RetryContext {
  /** Current attempt number (1-based) */
  attempt: number;
  /** Total elapsed time in milliseconds */
  elapsedMs: number;
  /** Previous errors from failed attempts */
  previousErrors: unknown[];
  /** Operation name/identifier */
  operationName: string;
  /** Custom context data */
  data?: Record<string, any>;
}

/**
 * Enhanced retry execution options
 */
export interface RetryExecutionOptions {
  /** Custom retry policy */
  policy?: Partial<RetryPolicy>;
  /** Custom retry condition */
  condition?: RetryCondition;
  /** Operation name for logging/monitoring */
  operationName?: string;
  /** Context data to pass through retries */
  contextData?: Record<string, any>;
  /** Callback before each retry attempt */
  onBeforeRetry?: (context: RetryContext) => void | Promise<void>;
  /** Callback after each failed attempt */
  onRetryFailed?: (error: unknown, context: RetryContext) => void | Promise<void>;
  /** Callback when all retries are exhausted */
  onRetriesExhausted?: (errors: unknown[], context: RetryContext) => void | Promise<void>;
}
