# ms-event-stream

A **NestJS package** that provides a **plug-and-play event system** for microservices with enterprise-grade reliability features.

## 🚀 Features

- **Outbox/Inbox Patterns** - Reliable event publishing and consuming with guaranteed delivery
- **Redis Streams** - High-performance event streaming with consumer groups
- **Event Deduplication** - Prevent duplicate event processing in distributed systems
- **Retry Logic** - Automatic retries with exponential backoff and Dead Letter Queue (DLQ)
- **Horizontal Scaling** - Support for multiple service instances with proper load balancing
- **Observability** - Built-in metrics, tracing, and structured logging
- **Type Safety** - Full TypeScript support with type-safe event handlers
- **Easy Integration** - Simple decorator-based event handling

## 📦 Installation

```bash
npm install ms-event-stream
```

## ⚡ Quick Start

### 1. Configure the Module

```typescript
import { Module } from '@nestjs/common';
import { EventStreamModule } from 'ms-event-stream';

@Module({
  imports: [
    EventStreamModule.forRoot({
      serviceName: 'order-service',
      dbUrl: 'postgresql://user:pass@localhost:5432/orders',
      redisUrl: 'redis://localhost:6379',
      env: 'production'
    })
  ],
})
export class AppModule {}
```

### 2. Publish Events

```typescript
import { Injectable } from '@nestjs/common';
import { EventStreamService } from 'ms-event-stream';

@Injectable()
export class OrderService {
  constructor(private readonly eventBus: EventStreamService) {}

  async createOrder(orderData: any) {
    // Your business logic
    const order = await this.saveOrder(orderData);

    // Publish event reliably
    await this.eventBus.publish('order.created', {
      orderId: order.id,
      customerId: order.customerId,
      amount: order.amount,
      items: order.items
    });
  }
}
```

### 3. Handle Events

```typescript
import { Injectable } from '@nestjs/common';
import { OnEvent, EventPayload } from 'ms-event-stream';

@Injectable()
export class PaymentService {
  @OnEvent('order.created')
  async handleOrderCreated(event: EventPayload<{ orderId: number; amount: number }>) {
    console.log('Processing payment for order:', event.data.orderId);
    
    // Your event handling logic
    await this.processPayment(event.data);
  }
}
```

## 🛠️ CLI Tool

The `ms-event-stream` package includes a CLI tool for basic system management and information.

### Installation

```bash
# Install CLI globally
npm install -g ms-event-stream

# Or use locally
npx ms-event-stream-cli
```

### Basic Usage

```bash
# Check system health
ms-event-stream-cli health

# Show version information
ms-event-stream-cli version

# Show configuration information
ms-event-stream-cli config

# Show system status
ms-event-stream-cli status
```

### Configuration

The CLI can be configured via:

1. **Environment variables**:
   - `MS_EVENT_STREAM_SERVICE_NAME`
   - `MS_EVENT_STREAM_DB_URL`
   - `MS_EVENT_STREAM_ENV`

2. **Command line options**:
   ```bash
   ms-event-stream-cli --verbose --json
   ```

### Available Commands

- **`health`** - Check system health
- **`version`** - Show version information
- **`config`** - Show configuration information
- **`status`** - Show system status

> **Note**: This CLI provides basic functionality. For full event stream management features (inspect, replay, purge, archive), use the ms-event-stream package in your NestJS application with the EventStreamModule.

## 🔧 Configuration

```typescript
interface EventStreamConfig {
  serviceName: string;    // Unique service identifier
  dbUrl: string;         // PostgreSQL connection URL
  redisUrl: string;      // Redis connection URL
  env?: 'development' | 'staging' | 'production';
}
```

### Advanced Configuration

```typescript
EventStreamModule.forRoot({
  serviceName: 'payment-service',
  dbUrl: 'postgresql://user:pass@localhost:5432/payments',
  redisUrl: 'redis://localhost:6379',
  env: 'production',
  
  // Database settings
  database: {
    maxConnections: 20,
    connectionTimeoutMs: 5000,
    idleTimeoutMs: 30000,
  },
  
  // Redis settings
  redis: {
    retryAttempts: 3,
    retryDelayMs: 1000,
    connectionTimeoutMs: 5000,
  },
  
  // Event processing settings
  events: {
    maxConcurrentEvents: 50,
    processingTimeoutMs: 30000,
    enableDeduplication: true,
  }
})
```

### Async Configuration

```typescript
EventStreamModule.forRootAsync({
  useFactory: async (configService: ConfigService) => ({
    serviceName: configService.get('SERVICE_NAME'),
    dbUrl: configService.get('DATABASE_URL'),
    redisUrl: configService.get('REDIS_URL'),
    env: configService.get('NODE_ENV'),
  }),
  inject: [ConfigService],
})
```

## 📊 Event Flow

```
[Service A] --publish--> [Outbox DB] --stream--> [Redis] --consume--> [Inbox DB] --process--> [Service B]
     ↓                      ↓                      ↓                     ↓                      ↓
  Business Logic      Transactional          High Performance       Deduplication         Event Handler
                      Guarantees             Message Broker         Protection            (@OnEvent)
```

## 🏗️ Architecture

- **Outbox Pattern**: Events are first saved to database, then published to Redis
- **Inbox Pattern**: Consumed events are stored in database to prevent duplicate processing  
- **Consumer Groups**: Redis consumer groups ensure each event is processed by only one instance
- **Retry & DLQ**: Failed events are automatically retried, then moved to Dead Letter Queue
- **Archival**: Old events are automatically archived to maintain performance

## ⚙️ Development Status

This package is currently in **Phase 1** of implementation. The following phases are planned:

- ✅ **Phase 1**: Core structure and interfaces (Current)
- 🚧 **Phase 2**: Database adapter and table creation
- 🚧 **Phase 3**: Redis adapter and streams
- 🚧 **Phase 4**: Outbox pattern implementation
- 🚧 **Phase 5**: Inbox pattern implementation
- 🚧 **Phase 6**: Event consumer system
- 🚧 **Phase 7**: @OnEvent decorator
- 🚧 **Phase 8**: Retry logic & error handling
- 🚧 **Phase 9**: Dead Letter Queue (DLQ)
- 🚧 **Phase 10**: Event archival system
- 🚧 **Phase 11**: Metrics & observability
- 🚧 **Phase 12**: Distributed tracing
- 🚧 **Phase 13**: Security features
- 🚧 **Phase 14**: Advanced scaling features
- ✅ **Phase 15**: CLI tool & developer experience

## 🧪 Testing

```bash
# Run tests
npm test

# Run tests in watch mode
npm run test:watch

# Run tests with coverage
npm run test:coverage
```

## 🔨 Development

```bash
# Install dependencies
npm install

# Build the package
npm run build

# Watch for changes
npm run build:watch

# Lint code
npm run lint

# Fix linting issues
npm run lint:fix
```

## 📄 License

MIT

## 🤝 Contributing

This package is part of a larger implementation plan. Please refer to the `implementation-breakdown.md` file for detailed phase information and contribution guidelines.
