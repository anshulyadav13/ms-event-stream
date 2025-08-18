import { Injectable, Logger, OnApplicationShutdown } from '@nestjs/common';
import { 
  createClient, 
  RedisClientType, 
  RedisDefaultModules, 
  RedisFunctions, 
  RedisModules, 
  RedisScripts 
} from 'redis';
import { ResolvedEventStreamConfig } from '../interfaces';

/**
 * Redis connection health status
 */
export interface RedisHealth {
  status: 'healthy' | 'unhealthy' | 'degraded';
  lastCheck: Date;
  error?: string;
  responseTimeMs?: number;
  memoryUsage?: string;
  connections?: number;
  keys?: number;
}

/**
 * Redis operation result
 */
export interface RedisResult<T = any> {
  success: boolean;
  data?: T | undefined;
  error?: string | undefined;
}

/**
 * Redis stream message structure
 */
export interface StreamMessage {
  id: string;
  message: Record<string, string>;
}

/**
 * Consumer group information
 */
export interface ConsumerGroupInfo {
  name: string;
  consumers: number;
  pending: number;
  lastDeliveredId: string;
}

/**
 * Stream information
 */
export interface StreamInfo {
  length: number;
  radixTreeKeys: number;
  radixTreeNodes: number;
  groups: number;
  lastGeneratedId: string;
  maxDeletedEntryId?: string | undefined;
  entriesAdded: number;
}

/**
 * Redis adapter for event streaming with automatic stream and consumer group management
 */
@Injectable()
export class RedisAdapter implements OnApplicationShutdown {
  private readonly logger = new Logger(RedisAdapter.name);
  private client!: RedisClientType<RedisDefaultModules & RedisModules, RedisFunctions, RedisScripts>;
  private isInitialized = false;
  private lastHealthCheck?: RedisHealth;
  private readonly serviceName: string;
  private readonly eventStreamName: string;
  private readonly dlqStreamName: string;
  private readonly consumerGroupName: string;
  private reconnectAttempts = 0;
  private readonly maxReconnectAttempts = 10;
  private reconnectTimer?: NodeJS.Timeout;

  constructor(private readonly config: ResolvedEventStreamConfig) {
    this.serviceName = config.serviceName;
    this.eventStreamName = `${this.serviceName}:events`;
    this.dlqStreamName = `${this.serviceName}:dlq`;
    this.consumerGroupName = `${this.serviceName}-consumers`;
    
    this.createClient();
  }

  /**
   * Create Redis client with configuration
   */
  private createClient(): void {
    this.logger.log('Creating Redis client...');

    this.client = createClient({
      url: this.config.redisUrl,
      socket: {
        connectTimeout: this.config.redis.connectionTimeoutMs,
        reconnectStrategy: (retries) => {
          if (retries > this.maxReconnectAttempts) {
            this.logger.error(`Maximum Redis reconnection attempts (${this.maxReconnectAttempts}) exceeded`);
            return false;
          }
          
          // Exponential backoff with jitter: 1s, 2s, 4s, 8s, ... up to 30s
          const delay = Math.min(Math.pow(2, retries) * 1000 + Math.random() * 1000, 30000);
          this.logger.warn(`Redis reconnection attempt ${retries + 1} in ${delay}ms...`);
          return delay;
        }
      },
      disableOfflineQueue: false,
    });

    this.setupEventListeners();
  }

  /**
   * Setup Redis client event listeners
   */
  private setupEventListeners(): void {
    this.client.on('connect', () => {
      this.logger.log('Redis client connected');
      this.reconnectAttempts = 0;
    });

    this.client.on('ready', () => {
      this.logger.log('Redis client ready');
    });

    this.client.on('error', (error) => {
      this.logger.error('Redis client error:', error);
      this.lastHealthCheck = {
        status: 'unhealthy',
        lastCheck: new Date(),
        error: error.message
      };
    });

    this.client.on('reconnecting', () => {
      this.reconnectAttempts++;
      this.logger.warn(`Redis client reconnecting (attempt ${this.reconnectAttempts})...`);
    });

    this.client.on('end', () => {
      this.logger.warn('Redis client connection ended');
    });
  }

  /**
   * Initialize Redis connection and setup streams
   */
  async initialize(): Promise<void> {
    if (this.isInitialized) {
      return;
    }

    this.logger.log('Initializing Redis adapter...');

    try {
      // Connect to Redis
      await this.connect();

      // Setup streams and consumer groups
      await this.setupStreams();

      this.isInitialized = true;
      this.logger.log('Redis adapter initialized successfully');

    } catch (error) {
      this.logger.error('Failed to initialize Redis adapter:', error);
      throw new Error(`Redis initialization failed: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * Connect to Redis with retry logic
   */
  private async connect(): Promise<void> {
    let lastError: Error | undefined;
    
    for (let attempt = 1; attempt <= this.config.redis.retryAttempts; attempt++) {
      try {
        await this.client.connect();
        
        // Test connection with PING
        const pong = await this.client.ping();
        if (pong !== 'PONG') {
          throw new Error('Redis PING test failed');
        }
        
        this.logger.log(`Redis connection successful (attempt ${attempt})`);
        return;
        
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        this.logger.warn(`Redis connection attempt ${attempt} failed:`, lastError.message);
        
        if (attempt < this.config.redis.retryAttempts) {
          const delay = this.config.redis.retryDelayMs * attempt;
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }
    
    throw new Error(`Redis connection failed after ${this.config.redis.retryAttempts} attempts: ${lastError?.message}`);
  }

  /**
   * Setup Redis streams and consumer groups
   */
  private async setupStreams(): Promise<void> {
    this.logger.log('Setting up Redis streams...');

    try {
      // Create event stream if it doesn't exist
      await this.createStreamIfNotExists(this.eventStreamName);
      
      // Create DLQ stream if it doesn't exist
      await this.createStreamIfNotExists(this.dlqStreamName);
      
      // Create consumer group for event stream
      await this.createConsumerGroupIfNotExists(this.eventStreamName, this.consumerGroupName);
      
      // Create consumer group for DLQ stream
      await this.createConsumerGroupIfNotExists(this.dlqStreamName, `${this.consumerGroupName}-dlq`);
      
      this.logger.log('Redis streams setup completed');
      
    } catch (error) {
      this.logger.error('Failed to setup Redis streams:', error);
      throw error;
    }
  }

  /**
   * Create a Redis stream if it doesn't exist
   */
  private async createStreamIfNotExists(streamName: string): Promise<void> {
    try {
      // Check if stream exists by trying to get its info
      await this.client.xInfoStream(streamName);
      this.logger.debug(`Stream ${streamName} already exists`);
      
    } catch (error) {
      // Stream doesn't exist, create it with a dummy message that we'll delete
      try {
        const messageId = await this.client.xAdd(streamName, '*', {
          '__init__': 'stream_created'
        });
        
        // Delete the dummy message
        await this.client.xDel(streamName, messageId);
        
        this.logger.log(`Created Redis stream: ${streamName}`);
        
      } catch (createError) {
        this.logger.error(`Failed to create stream ${streamName}:`, createError);
        throw createError;
      }
    }
  }

  /**
   * Create a consumer group if it doesn't exist
   */
  private async createConsumerGroupIfNotExists(streamName: string, groupName: string): Promise<void> {
    try {
      // Try to create the consumer group
      await this.client.xGroupCreate(streamName, groupName, '0', {
        MKSTREAM: true
      });
      
      this.logger.log(`Created consumer group: ${groupName} for stream: ${streamName}`);
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      // If group already exists, that's fine
      if (errorMessage.includes('BUSYGROUP') || errorMessage.includes('already exists')) {
        this.logger.debug(`Consumer group ${groupName} already exists for stream ${streamName}`);
      } else {
        this.logger.error(`Failed to create consumer group ${groupName}:`, error);
        throw error;
      }
    }
  }

  /**
   * Add a message to a Redis stream
   */
  async addToStream(
    streamName: string, 
    fields: Record<string, string | number | Buffer>,
    maxLength?: number
  ): Promise<RedisResult<string>> {
    try {
      const options: any = maxLength ? {
        MAXLEN: maxLength,
        APPROXIMATETRIMMING: true
      } : undefined;
      
      const messageId = await this.client.xAdd(streamName, '*', fields as Record<string, string>, options);
      
      this.logger.debug(`Message added to stream ${streamName}: ${messageId}`);
      
      return {
        success: true,
        data: messageId
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to add message to stream ${streamName}:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Read messages from a stream using consumer group
   */
  async readFromStream(
    streamName: string,
    groupName: string,
    consumerName: string,
    count: number = 10,
    blockMs: number = 1000
  ): Promise<RedisResult<StreamMessage[]>> {
    try {
      const result = await this.client.xReadGroup(
        groupName,
        consumerName,
        [
          {
            key: streamName,
            id: '>' // Read only new messages
          }
        ],
        {
          COUNT: count,
          BLOCK: blockMs
        }
      );
      
      if (!result || result.length === 0) {
        return {
          success: true,
          data: []
        };
      }
      
      const messages: StreamMessage[] = [];
      
      for (const stream of result) {
        for (const message of stream.messages) {
          messages.push({
            id: message.id,
            message: message.message
          });
        }
      }
      
      return {
        success: true,
        data: messages
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to read from stream ${streamName}:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Acknowledge message processing
   */
  async acknowledgeMessage(streamName: string, groupName: string, messageId: string): Promise<RedisResult<number>> {
    try {
      const result = await this.client.xAck(streamName, groupName, messageId);
      
      this.logger.debug(`Message acknowledged: ${messageId} from stream ${streamName}`);
      
      return {
        success: true,
        data: result
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to acknowledge message ${messageId}:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Get pending messages for a consumer group
   */
  async getPendingMessages(
    streamName: string,
    groupName: string,
    consumerName?: string,
    count: number = 10
  ): Promise<RedisResult<any[]>> {
    try {
      const result = await this.client.xPending(streamName, groupName);
      
      return {
        success: true,
        data: [result]
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to get pending messages:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Get stream information
   */
  async getStreamInfo(streamName: string): Promise<RedisResult<StreamInfo>> {
    try {
      const result = await this.client.xInfoStream(streamName);
      
      return {
        success: true,
        data: {
          length: result.length,
          radixTreeKeys: result.radixTreeKeys,
          radixTreeNodes: result.radixTreeNodes,
          groups: result.groups,
          lastGeneratedId: result.lastGeneratedId,
          maxDeletedEntryId: undefined,
          entriesAdded: result.length // Use length as approximation
        }
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Get consumer group information
   */
  async getConsumerGroupInfo(streamName: string): Promise<RedisResult<ConsumerGroupInfo[]>> {
    try {
      const result = await this.client.xInfoGroups(streamName);
      
      const groups: ConsumerGroupInfo[] = result.map((group: any) => ({
        name: group.name,
        consumers: group.consumers,
        pending: group.pending,
        lastDeliveredId: group.lastDeliveredId
      }));
      
      return {
        success: true,
        data: groups
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Read messages from a stream with range (for DLQ inspection)
   */
  async readStreamRange(
    streamName: string,
    options: {
      count?: number;
      startId?: string;
      endId?: string;
    } = {}
  ): Promise<RedisResult<StreamMessage[]>> {
    const { count = 100, startId = '-', endId = '+' } = options;
    
    try {
      const result = await this.client.xRange(streamName, startId, endId, {
        COUNT: count
      });
      
      const messages: StreamMessage[] = result.map((message: any) => ({
        id: message.id,
        message: message.message
      }));
      
      return {
        success: true,
        data: messages
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to read range from stream ${streamName}:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Delete messages from a stream
   */
  async deleteFromStream(streamName: string, messageIds: string[]): Promise<RedisResult<number>> {
    try {
      const result = await this.client.xDel(streamName, messageIds);
      
      this.logger.debug(`Deleted ${result} messages from stream ${streamName}`);
      
      return {
        success: true,
        data: result
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to delete messages from stream ${streamName}:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Check Redis health and connectivity
   */
  async checkHealth(): Promise<RedisHealth> {
    const startTime = Date.now();
    
    try {
      // Test basic connectivity
      const pong = await this.client.ping();
      if (pong !== 'PONG') {
        throw new Error('Redis PING test failed');
      }
      
      // Get Redis info for additional health metrics
      const info = await this.client.info('memory');
      const serverInfo = await this.client.info('clients');
      
      const responseTime = Date.now() - startTime;
      
      // Parse memory usage
      const memoryMatch = info.match(/used_memory_human:(.+)/);
      const memoryUsage = memoryMatch ? memoryMatch[1].trim() : 'unknown';
      
      // Parse connected clients
      const clientsMatch = serverInfo.match(/connected_clients:(\d+)/);
      const connections = clientsMatch ? parseInt(clientsMatch[1]) : 0;

      // Get total keys
      const dbSize = await this.client.dbSize();
      
      this.lastHealthCheck = {
        status: 'healthy',
        lastCheck: new Date(),
        responseTimeMs: responseTime,
        memoryUsage,
        connections,
        keys: dbSize
      };
      
    } catch (error) {
      const responseTime = Date.now() - startTime;
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.lastHealthCheck = {
        status: 'unhealthy',
        lastCheck: new Date(),
        error: errorMessage,
        responseTimeMs: responseTime
      };
      
      this.logger.error('Redis health check failed:', error);
    }
    
    return this.lastHealthCheck;
  }

  /**
   * Execute a custom Redis command
   */
  async executeCommand(command: string, ...args: any[]): Promise<RedisResult<any>> {
    try {
      const result = await this.client.sendCommand([command, ...args.map(String)]);
      
      return {
        success: true,
        data: result
      };
      
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      
      this.logger.error(`Failed to execute Redis command ${command}:`, error);
      
      return {
        success: false,
        error: errorMessage
      };
    }
  }

  /**
   * Get the last health check result
   */
  getLastHealthCheck(): RedisHealth | null {
    return this.lastHealthCheck || null;
  }

  /**
   * Get service-specific stream names
   */
  getStreamNames(): { events: string; dlq: string } {
    return {
      events: this.eventStreamName,
      dlq: this.dlqStreamName
    };
  }

  /**
   * Get consumer group name
   */
  getConsumerGroupName(): string {
    return this.consumerGroupName;
  }

  /**
   * Check if the adapter is initialized
   */
  isReady(): boolean {
    return this.isInitialized && this.client.isReady;
  }

  /**
   * Get service name
   */
  getServiceName(): string {
    return this.serviceName;
  }

  /**
   * Graceful shutdown - close Redis connection
   */
  async onApplicationShutdown(): Promise<void> {
    this.logger.log('Shutting down Redis adapter...');
    
    try {
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
      }
      
      if (this.client.isOpen) {
        await this.client.quit();
      }
      
      this.logger.log('Redis connection closed gracefully');
    } catch (error) {
      this.logger.error('Error during Redis shutdown:', error);
    }
  }

  /**
   * Force close Redis connection (emergency shutdown)
   */
  async forceShutdown(): Promise<void> {
    this.logger.warn('Force shutting down Redis adapter...');
    
    try {
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
      }
      
      this.client.removeAllListeners();
      
      if (this.client.isOpen) {
        await this.client.disconnect();
      }
      
      this.logger.log('Redis connection force closed');
    } catch (error) {
      this.logger.error('Error during force shutdown:', error);
    }
  }
}
