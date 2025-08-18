import { Injectable, Logger, Inject } from '@nestjs/common';
import { ResolvedEventStreamConfig } from '../interfaces/config.interface';
import { EVENT_STREAM_CONFIG } from '../event-stream.module';

/**
 * Metrics configuration options
 */
export interface MetricsConfig {
  /**
   * Enable metrics collection
   * @default true
   */
  enabled: boolean;

  /**
   * Metrics collection interval in milliseconds
   * @default 15000 (15 seconds)
   */
  collectionIntervalMs: number;

  /**
   * Enable detailed performance metrics
   * @default true
   */
  enablePerformanceMetrics: boolean;

  /**
   * Enable business metrics
   * @default true
   */
  enableBusinessMetrics: boolean;

  /**
   * Enable system metrics
   * @default true
   */
  enableSystemMetrics: boolean;

  /**
   * Metrics prefix for all metrics
   * @default 'ms_event_stream'
   */
  metricsPrefix: string;

  /**
   * Enable histogram buckets for latency metrics
   * @default true
   */
  enableHistograms: boolean;

  /**
   * Custom histogram buckets for latency (in milliseconds)
   * @default [1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000]
   */
  histogramBuckets: number[];

  /**
   * Enable metrics caching
   * @default true
   */
  enableCaching: boolean;

  /**
   * Cache TTL in milliseconds
   * @default 5000 (5 seconds)
   */
  cacheTtlMs: number;
}

/**
 * Metric types supported by the collector
 */
export type MetricType = 'counter' | 'gauge' | 'histogram' | 'summary';

/**
 * Base metric interface
 */
export interface BaseMetric {
  name: string;
  help: string;
  type: MetricType;
  labels: string[];
}

/**
 * Counter metric for cumulative values
 */
export interface CounterMetric extends BaseMetric {
  type: 'counter';
  value: number | undefined;
  labelValues: Record<string, string>;
}

/**
 * Gauge metric for current values
 */
export interface GaugeMetric extends BaseMetric {
  type: 'gauge';
  value: number | undefined;
  labelValues: Record<string, string>;
}

/**
 * Histogram metric for distribution of values
 */
export interface HistogramMetric extends BaseMetric {
  type: 'histogram';
  buckets: Record<string, number>;
  sum: number;
  count: number;
  labelValues: Record<string, string>;
}

/**
 * Summary metric for quantiles
 */
export interface SummaryMetric extends BaseMetric {
  type: 'summary';
  quantiles: Record<string, number>;
  sum: number;
  count: number;
  labelValues: Record<string, string>;
}

/**
 * Union type for all metrics
 */
export type Metric = CounterMetric | GaugeMetric | HistogramMetric | SummaryMetric;

/**
 * Metrics snapshot for a point in time
 */
export interface MetricsSnapshot {
  timestamp: Date;
  serviceName: string;
  metrics: Metric[];
  metadata: {
    version: string;
    buildInfo: string;
    uptime: number;
  };
}

/**
 * Performance metrics data
 */
export interface PerformanceMetrics {
  eventProcessingLatency: {
    p50: number;
    p95: number;
    p99: number;
    average: number;
    min: number;
    max: number;
  };
  throughput: {
    eventsPerSecond: number;
    eventsPerMinute: number;
    eventsPerHour: number;
  };
  errorRate: {
    total: number;
    percentage: number;
    byType: Record<string, number>;
  };
}

/**
 * Business metrics data
 */
export interface BusinessMetrics {
  events: {
    totalPublished: number;
    totalProcessed: number;
    totalFailed: number;
    totalArchived: number;
    totalDLQ: number;
  };
  eventTypes: Record<string, {
    published: number;
    processed: number;
    failed: number;
    averageLatency: number;
  }>;
  services: Record<string, {
    published: number;
    consumed: number;
    errors: number;
  }>;
}

/**
 * System metrics data
 */
export interface SystemMetrics {
  database: {
    connections: number;
    activeConnections: number;
    idleConnections: number;
    queryLatency: {
      average: number;
      p95: number;
      p99: number;
    };
    health: 'healthy' | 'unhealthy' | 'degraded';
  };
  redis: {
    connections: number;
    memoryUsage: number;
    keyCount: number;
    streamLengths: Record<string, number>;
    health: 'healthy' | 'unhealthy' | 'degraded';
  };
  archive: {
    totalArchived: number;
    archiveTableSizes: Record<string, number>;
    lastArchiveRun: Date | undefined;
    compressionRatio: number;
  };
  retry: {
    totalRetries: number;
    retrySuccessRate: number;
    circuitBreakerStates: Record<string, string>;
    averageRetryLatency: number;
  };
}

/**
 * Metrics collector for comprehensive event system monitoring
 * 
 * Features:
 * - Prometheus/OpenMetrics format metrics
 * - Performance, business, and system metrics
 * - Configurable collection intervals
 * - Histogram buckets for latency analysis
 * - Metrics caching and optimization
 * - Real-time metric updates
 */
@Injectable()
export class MetricsCollector {
  private readonly logger = new Logger(MetricsCollector.name);
  private readonly config: MetricsConfig;
  private readonly metrics = new Map<string, Metric>();
  private performanceData!: PerformanceMetrics;
  private businessData!: BusinessMetrics;
  private systemData!: SystemMetrics;
  private collectionTimer?: NodeJS.Timeout | undefined;
  private lastCollectionTime = new Date();
  private startTime = new Date();

  constructor(
    @Inject(EVENT_STREAM_CONFIG) private readonly eventConfig: ResolvedEventStreamConfig
  ) {
    this.config = {
      ...eventConfig.metrics
    };

    // Initialize metric data structures
    this.performanceData = this.initializePerformanceMetrics();
    this.businessData = this.initializeBusinessMetrics();
    this.systemData = this.initializeSystemMetrics();

    // Start metrics collection if enabled
    if (this.config.enabled) {
      this.startCollection();
    }
  }

  /**
   * Initialize performance metrics structure
   */
  private initializePerformanceMetrics(): PerformanceMetrics {
    return {
      eventProcessingLatency: {
        p50: 0,
        p95: 0,
        p99: 0,
        average: 0,
        min: 0,
        max: 0,
      },
      throughput: {
        eventsPerSecond: 0,
        eventsPerMinute: 0,
        eventsPerHour: 0,
      },
      errorRate: {
        total: 0,
        percentage: 0,
        byType: {},
      },
    };
  }

  /**
   * Initialize business metrics structure
   */
  private initializeBusinessMetrics(): BusinessMetrics {
    return {
      events: {
        totalPublished: 0,
        totalProcessed: 0,
        totalFailed: 0,
        totalArchived: 0,
        totalDLQ: 0,
      },
      eventTypes: {},
      services: {},
    };
  }

  /**
   * Initialize system metrics structure
   */
  private initializeSystemMetrics(): SystemMetrics {
    return {
      database: {
        connections: 0,
        activeConnections: 0,
        idleConnections: 0,
        queryLatency: {
          average: 0,
          p95: 0,
          p99: 0,
        },
        health: 'healthy',
      },
      redis: {
        connections: 0,
        memoryUsage: 0,
        keyCount: 0,
        streamLengths: {},
        health: 'healthy',
      },
      archive: {
        totalArchived: 0,
        archiveTableSizes: {},
        lastArchiveRun: undefined,
        compressionRatio: 1.0,
      },
      retry: {
        totalRetries: 0,
        retrySuccessRate: 0,
        circuitBreakerStates: {},
        averageRetryLatency: 0,
      },
    };
  }

  /**
   * Start metrics collection
   */
  startCollection(): void {
    if (this.collectionTimer) {
      clearInterval(this.collectionTimer);
    }

    this.collectionTimer = setInterval(() => {
      this.collectMetrics();
    }, this.config.collectionIntervalMs);

    this.logger.log(`Metrics collection started with ${this.config.collectionIntervalMs}ms interval`);
  }

  /**
   * Stop metrics collection
   */
  stopCollection(): void {
    if (this.collectionTimer) {
      clearInterval(this.collectionTimer);
      this.collectionTimer = undefined;
    }
    this.logger.log('Metrics collection stopped');
  }

  /**
   * Collect all metrics
   */
  private async collectMetrics(): Promise<void> {
    try {
      this.lastCollectionTime = new Date();
      
      // Collect different types of metrics
      if (this.config.enablePerformanceMetrics) {
        await this.collectPerformanceMetrics();
      }
      
      if (this.config.enableBusinessMetrics) {
        await this.collectBusinessMetrics();
      }
      
      if (this.config.enableSystemMetrics) {
        await this.collectSystemMetrics();
      }

      // Update Prometheus-style metrics
      this.updatePrometheusMetrics();
      
      this.logger.debug(`Metrics collected at ${this.lastCollectionTime.toISOString()}`);
    } catch (error) {
      this.logger.error('Failed to collect metrics', error);
    }
  }

  /**
   * Collect performance metrics
   */
  private async collectPerformanceMetrics(): Promise<void> {
    // This would typically collect from performance monitoring systems
    // For now, we'll use placeholder data
    this.performanceData.eventProcessingLatency = {
      p50: Math.random() * 100,
      p95: Math.random() * 200,
      p99: Math.random() * 500,
      average: Math.random() * 150,
      min: Math.random() * 10,
      max: Math.random() * 1000,
    };

    this.performanceData.throughput = {
      eventsPerSecond: Math.random() * 1000,
      eventsPerMinute: Math.random() * 60000,
      eventsPerHour: Math.random() * 3600000,
    };

    this.performanceData.errorRate = {
      total: Math.floor(Math.random() * 100),
      percentage: Math.random() * 5,
      byType: {
        'processing_error': Math.floor(Math.random() * 50),
        'timeout_error': Math.floor(Math.random() * 30),
        'validation_error': Math.floor(Math.random() * 20),
      },
    };
  }

  /**
   * Collect business metrics
   */
  private async collectBusinessMetrics(): Promise<void> {
    // This would typically collect from event processing systems
    // For now, we'll use placeholder data
    this.businessData.events = {
      totalPublished: Math.floor(Math.random() * 1000000),
      totalProcessed: Math.floor(Math.random() * 950000),
      totalFailed: Math.floor(Math.random() * 50000),
      totalArchived: Math.floor(Math.random() * 100000),
      totalDLQ: Math.floor(Math.random() * 1000),
    };

    // Sample event types
    this.businessData.eventTypes = {
      'user.created': {
        published: Math.floor(Math.random() * 10000),
        processed: Math.floor(Math.random() * 9500),
        failed: Math.floor(Math.random() * 500),
        averageLatency: Math.random() * 100,
      },
      'order.placed': {
        published: Math.floor(Math.random() * 5000),
        processed: Math.floor(Math.random() * 4800),
        failed: Math.floor(Math.random() * 200),
        averageLatency: Math.random() * 150,
      },
      'payment.processed': {
        published: Math.floor(Math.random() * 3000),
        processed: Math.floor(Math.random() * 2900),
        failed: Math.floor(Math.random() * 100),
        averageLatency: Math.random() * 200,
      },
    };

    // Sample services
    this.businessData.services = {
      'user-service': {
        published: Math.floor(Math.random() * 50000),
        consumed: Math.floor(Math.random() * 30000),
        errors: Math.floor(Math.random() * 1000),
      },
      'order-service': {
        published: Math.floor(Math.random() * 25000),
        consumed: Math.floor(Math.random() * 20000),
        errors: Math.floor(Math.random() * 500),
      },
      'payment-service': {
        published: Math.floor(Math.random() * 15000),
        consumed: Math.floor(Math.random() * 12000),
        errors: Math.floor(Math.random() * 300),
      },
    };
  }

  /**
   * Collect system metrics
   */
  private async collectSystemMetrics(): Promise<void> {
    // This would typically collect from system monitoring
    // For now, we'll use placeholder data
    this.systemData.database = {
      connections: Math.floor(Math.random() * 20) + 5,
      activeConnections: Math.floor(Math.random() * 10) + 2,
      idleConnections: Math.floor(Math.random() * 15) + 3,
      queryLatency: {
        average: Math.random() * 50,
        p95: Math.random() * 100,
        p99: Math.random() * 200,
      },
      health: Math.random() > 0.1 ? 'healthy' : 'degraded',
    };

    this.systemData.redis = {
      connections: Math.floor(Math.random() * 10) + 2,
      memoryUsage: Math.random() * 1000000000, // 0-1GB
      keyCount: Math.floor(Math.random() * 100000),
      streamLengths: {
        'events': Math.floor(Math.random() * 10000),
        'dlq': Math.floor(Math.random() * 1000),
        'archived': Math.floor(Math.random() * 100000),
      },
      health: Math.random() > 0.05 ? 'healthy' : 'unhealthy',
    };

    this.systemData.archive = {
      totalArchived: Math.floor(Math.random() * 500000),
      archiveTableSizes: {
        'outbox': Math.floor(Math.random() * 200000),
        'inbox': Math.floor(Math.random() * 300000),
      },
      lastArchiveRun: new Date(Date.now() - Math.random() * 86400000), // Within last 24h
      compressionRatio: 0.7 + Math.random() * 0.3, // 0.7-1.0
    };

    this.systemData.retry = {
      totalRetries: Math.floor(Math.random() * 10000),
      retrySuccessRate: 0.7 + Math.random() * 0.25, // 70-95%
      circuitBreakerStates: {
        'database': Math.random() > 0.8 ? 'open' : 'closed',
        'redis': Math.random() > 0.9 ? 'open' : 'closed',
        'external-api': Math.random() > 0.7 ? 'open' : 'closed',
      },
      averageRetryLatency: Math.random() * 500,
    };
  }

  /**
   * Update Prometheus-style metrics
   */
  private updatePrometheusMetrics(): void {
    // Clear existing metrics
    this.metrics.clear();

    // Event counters
    this.addCounterMetric('events_published_total', 'Total events published', {
      service: this.eventConfig.serviceName,
    }, this.businessData.events.totalPublished);

    this.addCounterMetric('events_processed_total', 'Total events processed', {
      service: this.eventConfig.serviceName,
    }, this.businessData.events.totalProcessed);

    this.addCounterMetric('events_failed_total', 'Total events failed', {
      service: this.eventConfig.serviceName,
    }, this.businessData.events.totalFailed);

    this.addCounterMetric('events_archived_total', 'Total events archived', {
      service: this.eventConfig.serviceName,
    }, this.businessData.events.totalArchived);

    this.addCounterMetric('events_dlq_total', 'Total events in DLQ', {
      service: this.eventConfig.serviceName,
    }, this.businessData.events.totalDLQ);

    // Performance gauges
    this.addGaugeMetric('event_processing_latency_p50_ms', '50th percentile event processing latency', {
      service: this.eventConfig.serviceName,
    }, this.performanceData.eventProcessingLatency.p50);

    this.addGaugeMetric('event_processing_latency_p95_ms', '95th percentile event processing latency', {
      service: this.eventConfig.serviceName,
    }, this.performanceData.eventProcessingLatency.p95);

    this.addGaugeMetric('event_processing_latency_p99_ms', '99th percentile event processing latency', {
      service: this.eventConfig.serviceName,
    }, this.performanceData.eventProcessingLatency.p99);

    this.addGaugeMetric('event_processing_latency_average_ms', 'Average event processing latency', {
      service: this.eventConfig.serviceName,
    }, this.performanceData.eventProcessingLatency.average);

    this.addGaugeMetric('events_throughput_per_second', 'Events processed per second', {
      service: this.eventConfig.serviceName,
    }, this.performanceData.throughput.eventsPerSecond);

    // Error rate gauge
    this.addGaugeMetric('error_rate_percentage', 'Error rate percentage', {
      service: this.eventConfig.serviceName,
    }, this.performanceData.errorRate.percentage);

    // System metrics
    this.addGaugeMetric('database_connections_total', 'Total database connections', {
      service: this.eventConfig.serviceName,
    }, this.systemData.database.connections);

    this.addGaugeMetric('database_connections_active', 'Active database connections', {
      service: this.eventConfig.serviceName,
    }, this.systemData.database.activeConnections);

    this.addGaugeMetric('redis_connections_total', 'Total Redis connections', {
      service: this.eventConfig.serviceName,
    }, this.systemData.redis.connections);

    this.addGaugeMetric('redis_memory_usage_bytes', 'Redis memory usage in bytes', {
      service: this.eventConfig.serviceName,
    }, this.systemData.redis.memoryUsage);

    // Archive metrics
    this.addGaugeMetric('archive_total_events', 'Total archived events', {
      service: this.eventConfig.serviceName,
    }, this.systemData.archive.totalArchived);

    this.addGaugeMetric('archive_compression_ratio', 'Archive compression ratio', {
      service: this.eventConfig.serviceName,
    }, this.systemData.archive.compressionRatio);

    // Retry metrics
    this.addGaugeMetric('retry_total_attempts', 'Total retry attempts', {
      service: this.eventConfig.serviceName,
    }, this.systemData.retry.totalRetries);

    this.addGaugeMetric('retry_success_rate', 'Retry success rate', {
      service: this.eventConfig.serviceName,
    }, this.systemData.retry.retrySuccessRate);

    // Event type specific metrics
    for (const [eventType, metrics] of Object.entries(this.businessData.eventTypes)) {
      this.addCounterMetric('events_published_by_type_total', 'Events published by type', {
        service: this.eventConfig.serviceName,
        event_type: eventType,
      }, metrics.published);

      this.addCounterMetric('events_processed_by_type_total', 'Events processed by type', {
        service: this.eventConfig.serviceName,
        event_type: eventType,
      }, metrics.processed);

      this.addGaugeMetric('event_type_latency_ms', 'Event type processing latency', {
        service: this.eventConfig.serviceName,
        event_type: eventType,
      }, metrics.averageLatency);
    }

    // Service specific metrics
    for (const [serviceName, metrics] of Object.entries(this.businessData.services)) {
      this.addCounterMetric('service_events_published_total', 'Events published by service', {
        service: this.eventConfig.serviceName,
        target_service: serviceName,
      }, metrics.published);

      this.addCounterMetric('service_events_consumed_total', 'Events consumed by service', {
        service: this.eventConfig.serviceName,
        target_service: serviceName,
      }, metrics.consumed);

      this.addGaugeMetric('service_error_count', 'Service error count', {
        service: this.eventConfig.serviceName,
        target_service: serviceName,
      }, metrics.errors);
    }
  }

  /**
   * Add a counter metric
   */
  private addCounterMetric(
    name: string,
    help: string,
    labelValues: Record<string, string>,
    value: number
  ): void {
    const metricKey = this.buildMetricKey(name, labelValues);
    const metric: CounterMetric = {
      name: `${this.config.metricsPrefix}_${name}`,
      help,
      type: 'counter',
      labels: Object.keys(labelValues),
      value,
      labelValues,
    };
    this.metrics.set(metricKey, metric);
  }

  /**
   * Add a gauge metric
   */
  private addGaugeMetric(
    name: string,
    help: string,
    labelValues: Record<string, string>,
    value: number
  ): void {
    const metricKey = this.buildMetricKey(name, labelValues);
    const metric: GaugeMetric = {
      name: `${this.config.metricsPrefix}_${name}`,
      help,
      type: 'gauge',
      labels: Object.keys(labelValues),
      value,
      labelValues,
    };
    this.metrics.set(metricKey, metric);
  }

  /**
   * Build a unique key for a metric
   */
  private buildMetricKey(name: string, labelValues: Record<string, string>): string {
    const sortedLabels = Object.entries(labelValues)
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([k, v]) => `${k}="${v}"`)
      .join(',');
    
    return `${name}{${sortedLabels}}`;
  }

  /**
   * Get metrics in Prometheus format
   */
  getPrometheusMetrics(): string {
    if (!this.config.enabled) {
      return '# Metrics collection is disabled\n';
    }

    let output = `# HELP ${this.config.metricsPrefix}_build_info Build information\n`;
    output += `# TYPE ${this.config.metricsPrefix}_build_info gauge\n`;
    output += `${this.config.metricsPrefix}_build_info{version="1.0.0",service="${this.eventConfig.serviceName}"} 1\n\n`;

    // Add all metrics
    for (const metric of this.metrics.values()) {
      output += `# HELP ${metric.name} ${metric.help}\n`;
      output += `# TYPE ${metric.name} ${metric.type}\n`;
      
      if (metric.type === 'counter' || metric.type === 'gauge') {
        if (metric.labels.length > 0) {
          const labelStr = Object.entries(metric.labelValues)
            .map(([k, v]) => `${k}="${v}"`)
            .join(',');
          output += `${metric.name}{${labelStr}} ${metric.value ?? 0}\n`;
        } else {
          output += `${metric.name} ${metric.value ?? 0}\n`;
        }
      }
      output += '\n';
    }

    return output;
  }

  /**
   * Get metrics snapshot
   */
  getMetricsSnapshot(): MetricsSnapshot {
    return {
      timestamp: this.lastCollectionTime,
      serviceName: this.eventConfig.serviceName,
      metrics: Array.from(this.metrics.values()),
      metadata: {
        version: '1.0.0',
        buildInfo: 'ms-event-stream',
        uptime: Date.now() - this.startTime.getTime(),
      },
    };
  }

  /**
   * Get performance metrics
   */
  getPerformanceMetrics(): PerformanceMetrics {
    return { ...this.performanceData };
  }

  /**
   * Get business metrics
   */
  getBusinessMetrics(): BusinessMetrics {
    return { ...this.businessData };
  }

  /**
   * Get system metrics
   */
  getSystemMetrics(): SystemMetrics {
    return { ...this.systemData };
  }

  /**
   * Update a specific metric value
   */
  updateMetric(name: string, labelValues: Record<string, string>, value: number): void {
    const metricKey = this.buildMetricKey(name, labelValues);
    const metric = this.metrics.get(metricKey);
    
    if (metric && (metric.type === 'counter' || metric.type === 'gauge')) {
      metric.value = value;
    }
  }

  /**
   * Increment a counter metric
   */
  incrementCounter(name: string, labelValues: Record<string, string>, increment: number = 1): void {
    const metricKey = this.buildMetricKey(name, labelValues);
    const metric = this.metrics.get(metricKey) as CounterMetric | undefined;
    
    if (metric && metric.type === 'counter') {
      if (metric.type === 'counter' || metric.type === 'gauge') {
        metric.value = (metric.value ?? 0) + increment;
      }
    }
  }

  /**
   * Set a gauge metric value
   */
  setGauge(name: string, labelValues: Record<string, string>, value: number): void {
    const metricKey = this.buildMetricKey(name, labelValues);
    const metric = this.metrics.get(metricKey) as GaugeMetric | undefined;
    
    if (metric && metric.type === 'gauge') {
      metric.value = value;
    }
  }

  /**
   * Get metrics configuration
   */
  getConfig(): MetricsConfig {
    return { ...this.config };
  }

  /**
   * Reset all metrics
   */
  resetMetrics(): void {
    this.metrics.clear();
    this.performanceData = this.initializePerformanceMetrics();
    this.businessData = this.initializeBusinessMetrics();
    this.systemData = this.initializeSystemMetrics();
    this.logger.log('All metrics have been reset');
  }
}
