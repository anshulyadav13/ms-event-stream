import { Injectable, Logger, Inject } from '@nestjs/common';
import { DatabaseAdapter } from '../adapters/db.adapter';
import { RedisAdapter } from '../adapters/redis.adapter';
import { ResolvedEventStreamConfig } from '../interfaces/config.interface';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';

/**
 * Health check configuration options
 */
export interface HealthCheckConfig {
  /**
   * Enable health checks
   * @default true
   */
  enabled: boolean;

  /**
   * Health check interval in milliseconds
   * @default 30000 (30 seconds)
   */
  checkIntervalMs: number;

  /**
   * Health check timeout in milliseconds
   * @default 5000 (5 seconds)
   */
  checkTimeoutMs: number;

  /**
   * Enable detailed health checks
   * @default true
   */
  enableDetailedChecks: boolean;

  /**
   * Enable performance health checks
   * @default true
   */
  enablePerformanceChecks: boolean;

  /**
   * Enable business logic health checks
   * @default true
   */
  enableBusinessChecks: boolean;

  /**
   * Health check failure threshold
   * @default 3
   */
  failureThreshold: number;

  /**
   * Health check recovery threshold
   * @default 2
   */
  recoveryThreshold: number;

  /**
   * Enable health check caching
   * @default true
   */
  enableCaching: boolean;

  /**
   * Cache TTL in milliseconds
   * @default 10000 (10 seconds)
   */
  cacheTtlMs: number;
}

/**
 * Health status enumeration
 */
export enum HealthStatus {
  HEALTHY = 'healthy',
  DEGRADED = 'degraded',
  UNHEALTHY = 'unhealthy',
  UNKNOWN = 'unknown'
}

/**
 * Health check result
 */
export interface HealthCheckResult {
  status: HealthStatus;
  timestamp: Date;
  duration: number;
  message?: string | undefined;
  details?: Record<string, any> | undefined;
  error?: string | undefined;
}

/**
 * Component health information
 */
export interface ComponentHealth {
  name: string;
  status: HealthStatus;
  lastCheck: Date;
  lastSuccess?: Date;
  lastFailure?: Date;
  consecutiveFailures: number;
  consecutiveSuccesses: number;
  averageResponseTime: number;
  errorRate: number;
  details?: Record<string, any>;
}

/**
 * Overall system health status
 */
export interface SystemHealth {
  overall: HealthStatus;
  timestamp: Date;
  uptime: number;
  version: string;
  components: Record<string, ComponentHealth>;
  summary: {
    totalComponents: number;
    healthyComponents: number;
    degradedComponents: number;
    unhealthyComponents: number;
    unknownComponents: number;
  };
  performance: {
    averageResponseTime: number;
    slowestComponent: string;
    fastestComponent: string;
  };
  recommendations?: string[] | undefined;
}

/**
 * Health check context
 */
export interface HealthCheckContext {
  timeout?: number;
  includeDetails?: boolean;
  includePerformance?: boolean;
  includeBusiness?: boolean;
}

/**
 * Health check endpoint response
 */
export interface HealthEndpointResponse {
  status: HealthStatus;
  timestamp: string;
  uptime: number;
  version: string;
  checks: Record<string, HealthCheckResult>;
  summary: {
    totalComponents: number;
    healthyComponents: number;
    degradedComponents: number;
    unhealthyComponents: number;
    unknownComponents: number;
  };
}

/**
 * Comprehensive health checker for the event system
 * 
 * Features:
 * - Database and Redis health monitoring
 * - Performance and business logic health checks
 * - Configurable health check intervals
 * - Health status caching and optimization
 * - Detailed health reporting and recommendations
 * - Failure threshold management
 */
@Injectable()
export class HealthChecker {
  private readonly logger = new Logger(HealthChecker.name);
  private readonly config: HealthCheckConfig;
  private readonly componentHealth = new Map<string, ComponentHealth>();
  private healthCheckTimer?: NodeJS.Timeout | undefined;
  private lastOverallHealth: HealthStatus = HealthStatus.UNKNOWN;
  private startTime = new Date();
  private healthCache?: {
    data: SystemHealth;
    timestamp: Date;
  } | undefined;

  constructor(
    private readonly dbAdapter: DatabaseAdapter,
    private readonly redisAdapter: RedisAdapter,
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig
  ) {
    this.config = {
      ...eventConfig.health
    };

    // Initialize component health tracking
    this.initializeComponentHealth();

    // Start health checks if enabled
    if (this.config.enabled) {
      this.startHealthChecks();
    }
  }

  /**
   * Initialize component health tracking
   */
  private initializeComponentHealth(): void {
    const components = [
      'database',
      'redis',
      'event_processing',
      'archive_system',
      'retry_system',
      'dlq_system'
    ];

    for (const component of components) {
      this.componentHealth.set(component, {
        name: component,
        status: HealthStatus.UNKNOWN,
        lastCheck: new Date(),
        consecutiveFailures: 0,
        consecutiveSuccesses: 0,
        averageResponseTime: 0,
        errorRate: 0,
      });
    }
  }

  /**
   * Start periodic health checks
   */
  startHealthChecks(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
    }

    this.healthCheckTimer = setInterval(() => {
      this.runHealthChecks();
    }, this.config.checkIntervalMs);

    this.logger.log(`Health checks started with ${this.config.checkIntervalMs}ms interval`);
  }

  /**
   * Stop periodic health checks
   */
  stopHealthChecks(): void {
    if (this.healthCheckTimer) {
      clearInterval(this.healthCheckTimer);
      this.healthCheckTimer = undefined;
    }
    this.logger.log('Health checks stopped');
  }

  /**
   * Run all health checks
   */
  private async runHealthChecks(): Promise<void> {
    try {
      this.logger.debug('Running periodic health checks');

      // Run component health checks
      await this.checkDatabaseHealth();
      await this.checkRedisHealth();
      await this.checkEventProcessingHealth();
      await this.checkArchiveSystemHealth();
      await this.checkRetrySystemHealth();
      await this.checkDLQSystemHealth();

      // Update overall health status
      this.updateOverallHealth();

      this.logger.debug('Periodic health checks completed');
    } catch (error) {
      this.logger.error('Failed to run health checks', error);
    }
  }

  /**
   * Check database health
   */
  private async checkDatabaseHealth(): Promise<void> {
    const startTime = Date.now();
    const component = 'database';
    const health = this.componentHealth.get(component)!;

    try {
      const dbHealth = await this.dbAdapter.checkHealth();
      const duration = Date.now() - startTime;

      if (dbHealth.status === 'healthy') {
        this.updateComponentHealth(component, HealthStatus.HEALTHY, duration, {
          connections: dbHealth.connectionCount,
          responseTime: dbHealth.responseTimeMs,
        });
      } else if (dbHealth.status === 'degraded') {
        this.updateComponentHealth(component, HealthStatus.DEGRADED, duration, {
          connections: dbHealth.connectionCount,
          responseTime: dbHealth.responseTimeMs,
          error: dbHealth.error,
        });
      } else {
        this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
          error: dbHealth.error,
        });
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
        error: error instanceof Error ? error.message : 'Unknown database error',
      });
    }
  }

  /**
   * Check Redis health
   */
  private async checkRedisHealth(): Promise<void> {
    const startTime = Date.now();
    const component = 'redis';
    const health = this.componentHealth.get(component)!;

    try {
      const redisHealth = await this.redisAdapter.checkHealth();
      const duration = Date.now() - startTime;

      if (redisHealth.status === 'healthy') {
        this.updateComponentHealth(component, HealthStatus.HEALTHY, duration, {
          connections: redisHealth.connections,
          memoryUsage: redisHealth.memoryUsage,
          keyCount: redisHealth.keys,
        });
      } else if (redisHealth.status === 'degraded') {
        this.updateComponentHealth(component, HealthStatus.DEGRADED, duration, {
          connections: redisHealth.connections,
          memoryUsage: redisHealth.memoryUsage,
          error: redisHealth.error,
        });
      } else {
        this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
          error: redisHealth.error,
        });
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
        error: error instanceof Error ? error.message : 'Unknown Redis error',
      });
    }
  }

  /**
   * Check event processing health
   */
  private async checkEventProcessingHealth(): Promise<void> {
    const startTime = Date.now();
    const component = 'event_processing';
    const health = this.componentHealth.get(component)!;

    try {
      // Check if event processing is working
      // This would typically check queue depths, processing rates, etc.
      const duration = Date.now() - startTime;
      
      // For now, assume healthy if no errors in recent checks
      if (health.consecutiveFailures < this.config.failureThreshold) {
        this.updateComponentHealth(component, HealthStatus.HEALTHY, duration, {
          consecutiveSuccesses: health.consecutiveSuccesses,
          errorRate: health.errorRate,
        });
      } else {
        this.updateComponentHealth(component, HealthStatus.DEGRADED, duration, {
          consecutiveFailures: health.consecutiveFailures,
          errorRate: health.errorRate,
        });
      }
    } catch (error) {
      const duration = Date.now() - startTime;
      this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
        error: error instanceof Error ? error.message : 'Unknown event processing error',
      });
    }
  }

  /**
   * Check archive system health
   */
  private async checkArchiveSystemHealth(): Promise<void> {
    const startTime = Date.now();
    const component = 'archive_system';
    const health = this.componentHealth.get(component)!;

    try {
      // Check archive system health
      // This would typically check archive table sizes, compression ratios, etc.
      const duration = Date.now() - startTime;
      
      // For now, assume healthy
      this.updateComponentHealth(component, HealthStatus.HEALTHY, duration, {
        lastCheck: new Date().toISOString(),
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
        error: error instanceof Error ? error.message : 'Unknown archive system error',
      });
    }
  }

  /**
   * Check retry system health
   */
  private async checkRetrySystemHealth(): Promise<void> {
    const startTime = Date.now();
    const component = 'retry_system';
    const health = this.componentHealth.get(component)!;

    try {
      // Check retry system health
      // This would typically check retry success rates, circuit breaker states, etc.
      const duration = Date.now() - startTime;
      
      // For now, assume healthy
      this.updateComponentHealth(component, HealthStatus.HEALTHY, duration, {
        lastCheck: new Date().toISOString(),
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
        error: error instanceof Error ? error.message : 'Unknown retry system error',
      });
    }
  }

  /**
   * Check DLQ system health
   */
  private async checkDLQSystemHealth(): Promise<void> {
    const startTime = Date.now();
    const component = 'dlq_system';
    const health = this.componentHealth.get(component)!;

    try {
      // Check DLQ system health
      // This would typically check DLQ queue sizes, error rates, etc.
      const duration = Date.now() - startTime;
      
      // For now, assume healthy
      this.updateComponentHealth(component, HealthStatus.HEALTHY, duration, {
        lastCheck: new Date().toISOString(),
      });
    } catch (error) {
      const duration = Date.now() - startTime;
      this.updateComponentHealth(component, HealthStatus.UNHEALTHY, duration, {
        error: error instanceof Error ? error.message : 'Unknown DLQ system error',
      });
    }
  }

  /**
   * Update component health status
   */
  private updateComponentHealth(
    component: string,
    status: HealthStatus,
    responseTime: number,
    details?: Record<string, any>
  ): void {
    const health = this.componentHealth.get(component)!;
    const now = new Date();

    // Update status
    health.status = status;
    health.lastCheck = now;

    // Update consecutive counts
    if (status === HealthStatus.HEALTHY) {
      health.consecutiveSuccesses++;
      health.consecutiveFailures = 0;
      health.lastSuccess = now;
    } else {
      health.consecutiveFailures++;
      health.consecutiveSuccesses = 0;
      health.lastFailure = now;
    }

    // Update response time
    health.averageResponseTime = this.calculateAverageResponseTime(
      health.averageResponseTime,
      responseTime,
      health.consecutiveSuccesses + health.consecutiveFailures
    );

    // Update error rate
    health.errorRate = health.consecutiveFailures / (health.consecutiveSuccesses + health.consecutiveFailures);

    // Update details
    if (details) {
      health.details = { ...health.details, ...details };
    }

    this.logger.debug(`Component ${component} health updated: ${status} (${responseTime}ms)`);
  }

  /**
   * Calculate average response time
   */
  private calculateAverageResponseTime(
    currentAverage: number,
    newValue: number,
    totalChecks: number
  ): number {
    if (totalChecks === 1) {
      return newValue;
    }
    return (currentAverage * (totalChecks - 1) + newValue) / totalChecks;
  }

  /**
   * Update overall health status
   */
  private updateOverallHealth(): void {
    const components = Array.from(this.componentHealth.values());
    const healthyCount = components.filter(c => c.status === HealthStatus.HEALTHY).length;
    const degradedCount = components.filter(c => c.status === HealthStatus.DEGRADED).length;
    const unhealthyCount = components.filter(c => c.status === HealthStatus.UNHEALTHY).length;

    let newStatus: HealthStatus;
    if (unhealthyCount > 0) {
      newStatus = HealthStatus.UNHEALTHY;
    } else if (degradedCount > 0) {
      newStatus = HealthStatus.DEGRADED;
    } else if (healthyCount === components.length) {
      newStatus = HealthStatus.HEALTHY;
    } else {
      newStatus = HealthStatus.UNKNOWN;
    }

    if (newStatus !== this.lastOverallHealth) {
      this.logger.log(`Overall health status changed: ${this.lastOverallHealth} -> ${newStatus}`);
      this.lastOverallHealth = newStatus;
    }
  }

  /**
   * Get overall system health
   */
  async getSystemHealth(context?: HealthCheckContext): Promise<SystemHealth> {
    // Check cache first
    if (this.config.enableCaching && this.healthCache) {
      const cacheAge = Date.now() - this.healthCache.timestamp.getTime();
      if (cacheAge < this.config.cacheTtlMs) {
        return this.healthCache.data;
      }
    }

    // Run fresh health checks if requested
    if (context?.includeDetails) {
      await this.runHealthChecks();
    }

    const components = Array.from(this.componentHealth.values());
    const healthyCount = components.filter(c => c.status === HealthStatus.HEALTHY).length;
    const degradedCount = components.filter(c => c.status === HealthStatus.DEGRADED).length;
    const unhealthyCount = components.filter(c => c.status === HealthStatus.UNHEALTHY).length;
    const unknownCount = components.filter(c => c.status === HealthStatus.UNKNOWN).length;

    // Calculate performance metrics
    const responseTimes = components.map(c => c.averageResponseTime).filter(t => t > 0);
    const averageResponseTime = responseTimes.length > 0 
      ? responseTimes.reduce((a, b) => a + b, 0) / responseTimes.length 
      : 0;

    const slowestComponent = components.reduce((slowest, current) => 
      current.averageResponseTime > slowest.averageResponseTime ? current : slowest
    ).name;

    const fastestComponent = components.reduce((fastest, current) => 
      current.averageResponseTime < fastest.averageResponseTime ? current : fastest
    ).name;

    // Generate recommendations
    const recommendations: string[] = [];
    if (unhealthyCount > 0) {
      recommendations.push('Some components are unhealthy. Check logs for errors.');
    }
    if (degradedCount > 0) {
      recommendations.push('Some components are degraded. Monitor performance.');
    }
    if (averageResponseTime > 1000) {
      recommendations.push('System response times are high. Consider optimization.');
    }

    const systemHealth: SystemHealth = {
      overall: this.lastOverallHealth,
      timestamp: new Date(),
      uptime: Date.now() - this.startTime.getTime(),
      version: '1.0.0',
      components: Object.fromEntries(this.componentHealth.entries()),
      summary: {
        totalComponents: components.length,
        healthyComponents: healthyCount,
        degradedComponents: degradedCount,
        unhealthyComponents: unhealthyCount,
        unknownComponents: unknownCount,
      },
      performance: {
        averageResponseTime,
        slowestComponent,
        fastestComponent,
      },
      recommendations: recommendations.length > 0 ? recommendations : undefined,
    };

    // Update cache
    if (this.config.enableCaching) {
      this.healthCache = {
        data: systemHealth,
        timestamp: new Date(),
      };
    }

    return systemHealth;
  }

  /**
   * Get health check result for a specific component
   */
  async getComponentHealth(componentName: string): Promise<HealthCheckResult | null> {
    const health = this.componentHealth.get(componentName);
    if (!health) {
      return null;
    }

    const startTime = Date.now();
    let status = health.status;
    let message: string | undefined;
    let details: Record<string, any> | undefined;
    let error: string | undefined;

    try {
      // Run a quick health check for the specific component
      switch (componentName) {
        case 'database':
          const dbHealth = await this.dbAdapter.checkHealth();
          status = dbHealth.status === 'healthy' ? HealthStatus.HEALTHY : 
                   dbHealth.status === 'degraded' ? HealthStatus.DEGRADED : HealthStatus.UNHEALTHY;
          details = { connections: dbHealth.connectionCount, responseTime: dbHealth.responseTimeMs };
          break;
        case 'redis':
          const redisHealth = await this.redisAdapter.checkHealth();
          status = redisHealth.status === 'healthy' ? HealthStatus.HEALTHY : 
                   redisHealth.status === 'degraded' ? HealthStatus.DEGRADED : HealthStatus.UNHEALTHY;
          details = { connections: redisHealth.connections, memoryUsage: redisHealth.memoryUsage };
          break;
        default:
          // For other components, use cached status
          break;
      }
    } catch (err) {
      status = HealthStatus.UNHEALTHY;
      error = err instanceof Error ? err.message : 'Unknown error';
    }

    const duration = Date.now() - startTime;

    return {
      status,
      timestamp: new Date(),
      duration,
      message,
      details,
      error,
    };
  }

  /**
   * Get health endpoint response (for HTTP endpoints)
   */
  async getHealthEndpointResponse(): Promise<HealthEndpointResponse> {
    const systemHealth = await this.getSystemHealth();
    const checks: Record<string, HealthCheckResult> = {};

    // Convert component health to check results
    for (const [name, health] of Object.entries(systemHealth.components)) {
      checks[name] = {
        status: health.status,
        timestamp: health.lastCheck,
        duration: health.averageResponseTime,
        details: health.details,
      };
    }

    return {
      status: systemHealth.overall,
      timestamp: systemHealth.timestamp.toISOString(),
      uptime: systemHealth.uptime,
      version: systemHealth.version,
      checks,
      summary: systemHealth.summary,
    };
  }

  /**
   * Check if system is healthy
   */
  isHealthy(): boolean {
    return this.lastOverallHealth === HealthStatus.HEALTHY;
  }

  /**
   * Check if system is degraded
   */
  isDegraded(): boolean {
    return this.lastOverallHealth === HealthStatus.DEGRADED;
  }

  /**
   * Check if system is unhealthy
   */
  isUnhealthy(): boolean {
    return this.lastOverallHealth === HealthStatus.UNHEALTHY;
  }

  /**
   * Get health configuration
   */
  getConfig(): HealthCheckConfig {
    return { ...this.config };
  }

  /**
   * Reset health check data
   */
  resetHealthData(): void {
    this.initializeComponentHealth();
    this.lastOverallHealth = HealthStatus.UNKNOWN;
    this.healthCache = undefined;
    this.logger.log('Health check data has been reset');
  }

  /**
   * Force a health check refresh
   */
  async forceHealthCheck(): Promise<SystemHealth> {
    this.logger.log('Forcing health check refresh');
    await this.runHealthChecks();
    return await this.getSystemHealth();
  }
}
