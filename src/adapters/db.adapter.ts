import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { Pool, PoolClient, PoolConfig } from 'pg';
import { ResolvedEventStreamConfig } from '../interfaces';
import { 
  ALL_SCHEMAS, 
  PERFORMANCE_INDEXES, 
  TABLE_NAMES,
  EventStatus 
} from '../schemas/database.schema';

/**
 * Database connection health status
 */
export interface DatabaseHealth {
  status: 'healthy' | 'unhealthy' | 'degraded';
  lastCheck: Date;
  error?: string;
  connectionCount?: number;
  responseTimeMs?: number;
}

/**
 * Database operation result
 */
export interface DatabaseResult<T = any> {
  success: boolean;
  data?: T;
  error?: string;
  affectedRows?: number;
}

/**
 * Event record interfaces for type safety
 */
export interface OutboxEventRecord {
  event_id: string;
  event_type: string;
  payload: Record<string, any>;
  status: EventStatus;
  service_name: string;
  created_at: Date;
  updated_at: Date;
  retry_count: number;
  last_retry_at?: Date;
  error_message?: string;
}

export interface InboxEventRecord {
  event_id: string;
  event_type: string;
  payload: Record<string, any>;
  status: EventStatus;
  service_name: string;
  received_at: Date;
  processed_at?: Date;
  error_message?: string;
  retry_count: number;
}

/**
 * Database adapter for PostgreSQL with connection pooling and auto-schema creation
 */
@Injectable()
export class DatabaseAdapter implements OnApplicationShutdown {
  private readonly logger = new Logger(DatabaseAdapter.name);
  private pool!: Pool;
  private isInitialized = false;
  private lastHealthCheck?: DatabaseHealth;
  private readonly serviceName: string;

  constructor(private readonly config: ResolvedEventStreamConfig) {
    this.serviceName = config.serviceName;
    this.initializePool();
  }

  /**
   * Initialize the PostgreSQL connection pool
   */
  private initializePool(): void {
    const url = new URL(this.config.dbUrl);
    
    const poolConfig: PoolConfig = {
      host: url.hostname,
      port: parseInt(url.port) || 5432,
      database: url.pathname.slice(1), // Remove leading '/'
      user: url.username,
      password: url.password,
      max: this.config.database.maxConnections,
      connectionTimeoutMillis: this.config.database.connectionTimeoutMs,
      idleTimeoutMillis: this.config.database.idleTimeoutMs,
      // Additional optimizations
      allowExitOnIdle: true,
      keepAlive: true,
      statement_timeout: 30000, // 30 seconds
      query_timeout: 30000, // 30 seconds
    };

    this.pool = new Pool(poolConfig);

    // Pool event listeners for monitoring
    this.pool.on('connect', (client) => {
      this.logger.debug('New database client connected');
    });

    this.pool.on('error', (err, client) => {
      this.logger.error('Database pool error:', err);
    });

    this.pool.on('acquire', (client) => {
      this.logger.debug('Client acquired from pool');
    });

    this.pool.on('release', (client) => {
      this.logger.debug('Client released back to pool');
    });
  }

  /**
   * Initialize database connection and create tables
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    this.logger.log('Initializing database connection...');

    try {
      // Test connection
      await this.testConnection();

      // Create schemas and tables
      await this.createTables();

      // Create performance indexes
      await this.createIndexes();

      // Record initialization in metadata
      await this.recordServiceInitialization();

      this.isInitialized = true;
      this.logger.log('Database adapter initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize database adapter:', error);
      throw new Error(`Database initialization failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Test database connection with retry logic
   */
  private async testConnection(maxRetries = 3): Promise<void> {
    let lastError: Error | undefined;
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const client = await this.pool.connect();
        await client.query('SELECT NOW()');
        client.release();
        
        this.logger.log(`Database connection successful (attempt ${attempt})`);
        return;
        
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        this.logger.warn(`Database connection attempt ${attempt} failed:`, lastError.message);
        
        if (attempt < maxRetries) {
          // Exponential backoff: 1s, 2s, 4s
          const delay = Math.pow(2, attempt - 1) * 1000;
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    
    throw new Error(`Database connection failed after ${maxRetries} attempts: ${lastError?.message}`);
  }

  /**
   * Create all required database tables
   */
  private async createTables(): Promise<void> {
    this.logger.log('Creating database tables...');
    
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');
      
      for (const schema of ALL_SCHEMAS) {
        await client.query(schema);
      }
      
      await client.query('COMMIT');
      this.logger.log('Database tables created successfully');
      
    } catch (error) {
      await client.query('ROLLBACK');
      this.logger.error('Failed to create database tables:', error);
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Create performance indexes
   */
  private async createIndexes(): Promise<void> {
    this.logger.log('Creating database indexes...');
    
    const client = await this.pool.connect();
    
    try {
      for (const [name, indexSql] of Object.entries(PERFORMANCE_INDEXES)) {
        try {
          await client.query(indexSql);
          this.logger.debug(`Created index: ${name}`);
        } catch (error) {
          // Indexes might already exist, log warning but continue
          const errorMessage = error instanceof Error ? error.message : String(error);
          this.logger.warn(`Failed to create index ${name}:`, errorMessage);
        }
      }
      
      this.logger.log('Database indexes created successfully');
      
    } finally {
      client.release();
    }
  }

  /**
   * Record service initialization in metadata table
   */
  private async recordServiceInitialization(): Promise<void> {
    const client = await this.pool.connect();
    
    try {
      await client.query(`
        INSERT INTO ${TABLE_NAMES.EVENT_SYSTEM_METADATA} 
        (key, value, service_name, created_at, updated_at)
        VALUES ($1, $2, $3, NOW(), NOW())
        ON CONFLICT (key) DO UPDATE SET
          value = EXCLUDED.value,
          updated_at = NOW()
      `, [
        `service_initialized_${this.serviceName}`,
        JSON.stringify({
          version: '1.0.0',
          initializedAt: new Date().toISOString(),
          environment: this.config.env
        }),
        this.serviceName
      ]);
      
    } finally {
      client.release();
    }
  }

  /**
   * Execute a query with connection management
   */
  async query<T = any>(
    text: string, 
    params: any[] = []
  ): Promise<DatabaseResult<T[]>> {
    const startTime = Date.now();
    let client: PoolClient;
    
    try {
      client = await this.pool.connect();
      const result = await client.query(text, params);
      
      const responseTime = Date.now() - startTime;
      
      this.logger.debug(`Query executed successfully in ${responseTime}ms`);
      
      return {
        success: true,
        data: result.rows,
        affectedRows: result.rowCount || 0
      };
      
    } catch (error) {
      const responseTime = Date.now() - startTime;
      
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Query failed after ${responseTime}ms:`, {
        error: errorMessage,
        query: text,
        params
      });
      
      return {
        success: false,
        error: errorMessage
      };
      
    } finally {
      if (client!) {
        client.release();
      }
    }
  }

  /**
   * Execute multiple queries in a transaction
   */
  async transaction<T = any>(
    queries: Array<{ text: string; params?: any[] }>
  ): Promise<DatabaseResult<T[]>> {
    const client = await this.pool.connect();
    
    try {
      await client.query('BEGIN');
      
      const results: any[] = [];
      
      for (const query of queries) {
        const result = await client.query(query.text, query.params || []);
        results.push(...result.rows);
      }
      
      await client.query('COMMIT');
      
      return {
        success: true,
        data: results,
        affectedRows: results.length
      };
      
    } catch (error) {
      await client.query('ROLLBACK');
      
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error('Transaction failed:', error);
      
      return {
        success: false,
        error: errorMessage
      };
      
    } finally {
      client.release();
    }
  }

  /**
   * Check database health and connectivity
   */
  async checkHealth(): Promise<DatabaseHealth> {
    const startTime = Date.now();
    
    try {
      const client = await this.pool.connect();
      
      try {
        // Simple health check query
        await client.query('SELECT 1');
        
        const responseTime = Date.now() - startTime;
        const connectionCount = this.pool.totalCount;
        
        this.lastHealthCheck = {
          status: 'healthy',
          lastCheck: new Date(),
          connectionCount,
          responseTimeMs: responseTime
        };
        
      } finally {
        client.release();
      }
      
    } catch (error) {
      const responseTime = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.lastHealthCheck = {
        status: 'unhealthy',
        lastCheck: new Date(),
        error: errorMessage,
        responseTimeMs: responseTime
      };
      
      this.logger.error('Database health check failed:', error);
    }
    
    return this.lastHealthCheck;
  }

  /**
   * Get connection pool statistics
   */
  getPoolStats() {
    return {
      totalCount: this.pool.totalCount,
      idleCount: this.pool.idleCount,
      waitingCount: this.pool.waitingCount,
      maxConnections: this.config.database.maxConnections
    };
  }

  /**
   * Get the last health check result
   */
  getLastHealthCheck(): DatabaseHealth | null {
    return this.lastHealthCheck || null;
  }

  /**
   * Graceful shutdown - close all connections
   */
  async onApplicationShutdown(): Promise<void> {
    this.logger.log('Shutting down database adapter...');
    
    try {
      await this.pool.end();
      this.logger.log('Database connections closed gracefully');
    } catch (error) {
      this.logger.error('Error during database shutdown:', error);
    }
  }

  /**
   * Force close all connections (emergency shutdown)
   */
  async forceShutdown(): Promise<void> {
    this.logger.warn('Force shutting down database adapter...');
    
    try {
      // Destroy all connections immediately
      this.pool.removeAllListeners();
      await this.pool.end();
      this.logger.log('Database connections force closed');
    } catch (error) {
      this.logger.error('Error during force shutdown:', error);
    }
  }

  /**
   * Check if the adapter is initialized
   */
  isReady(): boolean {
    return this.isInitialized;
  }

  /**
   * Get service name
   */
  getServiceName(): string {
    return this.serviceName;
  }

  /**
   * Get a database client from the pool
   */
  async getClient(): Promise<PoolClient> {
    return await this.pool.connect();
  }
}
