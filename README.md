# ms-event-stream

A **production-ready NestJS package** that provides a **plug-and-play event system** for microservices with enterprise-grade reliability features.

## 🚀 Features

- **Outbox/Inbox Patterns** - Reliable event publishing and consuming with guaranteed delivery
- **Redis Streams** - High-performance event streaming with consumer groups
- **Event Deduplication** - Prevent duplicate event processing in distributed systems
- **Retry Logic** - Automatic retries with exponential backoff and Dead Letter Queue (DLQ)
- **Horizontal Scaling** - Support for multiple service instances with proper load balancing
- **Observability** - Built-in metrics, tracing, and structured logging
- **Type Safety** - Full TypeScript support with type-safe event handlers
- **Easy Integration** - Simple decorator-based event handling
- **Schema Validation** - JSON Schema validation for event payloads
- **Version Management** - Event schema versioning and migration support
- **Point-in-Time Recovery** - Replay events from any point in time
- **CLI Tool** - Built-in CLI for monitoring and management

## 📦 Installation

```bash
npm install ms-event-stream
```

**🎉 Package is now published on npm!** 
- **npm**: https://www.npmjs.com/package/ms-event-stream
- **GitHub**: https://github.com/anshulyadav13/ms-event-stream

## ⚡ Quick Start

### 1. Configure the Module

```typescript
import { Module } from '@nestjs/common';
import { EventStreamModule } from 'ms-event-stream';

@Module({
  imports: [
    EventStreamModule.forRoot({
      serviceName: 'user-service',
      dbUrl: 'postgresql://user:pass@localhost:5432/user_events',
      redisUrl: 'redis://localhost:6379',
      env: 'production',
      
      // Advanced configuration
      database: {
        maxConnections: 20,
        connectionTimeoutMs: 5000,
      },
      redis: {
        retryAttempts: 3,
        retryDelayMs: 1000,
      },
      events: {
        maxConcurrentEvents: 50,
        processingTimeoutMs: 30000,
        enableDeduplication: true,
      }
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
export class UserService {
  constructor(private readonly eventBus: EventStreamService) {}

  async createUser(userData: any) {
    // Your business logic
    const user = await this.saveUser(userData);

    // Publish event reliably
    await this.eventBus.publish('user.created', {
      userId: user.id,
      email: user.email,
      name: user.name,
      createdAt: user.createdAt
    });
  }

  async updateUserSubscription(userId: string, subscriptionData: any) {
    // Update user subscription
    const user = await this.updateSubscription(userId, subscriptionData);

    // Publish subscription event
    await this.eventBus.publish('user.subscription.updated', {
      userId: user.id,
      subscriptionId: user.subscriptionId,
      plan: user.plan,
      status: user.status,
      nextBillingDate: user.nextBillingDate,
      amount: user.amount
    });
  }
}

### 3. Handle Events

```typescript
import { Injectable } from '@nestjs/common';
import { OnEvent, EventPayload } from 'ms-event-stream';

@Injectable()
export class PaymentService {
  @OnEvent('user.subscription.updated')
  async handleSubscriptionUpdated(event: EventPayload<{
    userId: string;
    subscriptionId: string;
    plan: string;
    status: string;
    nextBillingDate: Date;
    amount: number;
  }>) {
    console.log('Processing payment for subscription:', event.data.subscriptionId);
    
    // Process payment based on subscription
    if (event.data.status === 'active') {
      await this.processSubscriptionPayment(event.data);
    } else if (event.data.status === 'cancelled') {
      await this.handleSubscriptionCancellation(event.data);
    }
  }

  @OnEvent('user.created')
  async handleUserCreated(event: EventPayload<{
    userId: string;
    email: string;
    name: string;
    createdAt: Date;
  }>) {
    console.log('New user created:', event.data.email);
    
    // Set up default payment methods or welcome offers
    await this.setupDefaultPaymentMethods(event.data.userId);
  }
}
```

## 🏗️ Real-World Example: User & Payment Microservices

### Architecture Overview

```
[User Service] --publish--> [Event Stream] --consume--> [Payment Service]
     ↓                           ↓                           ↓
  User Management           Redis Streams + DB           Payment Processing
  Subscription Updates      Outbox/Inbox Pattern        Billing & Invoicing
```

### User Service Implementation

```typescript
// user-service/src/user/user.service.ts
import { Injectable } from '@nestjs/common';
import { EventStreamService } from 'ms-event-stream';

@Injectable()
export class UserService {
  constructor(private readonly eventBus: EventStreamService) {}

  async createUser(userData: CreateUserDto) {
    // Create user in database
    const user = await this.userRepository.create(userData);
    
    // Publish user.created event
    await this.eventBus.publish('user.created', {
      userId: user.id,
      email: user.email,
      name: user.name,
      createdAt: user.createdAt,
      metadata: {
        source: 'user-service',
        version: '1.0.0'
      }
    });

    return user;
  }

  async updateSubscription(userId: string, subscriptionData: UpdateSubscriptionDto) {
    // Update subscription in database
    const user = await this.userRepository.updateSubscription(userId, subscriptionData);
    
    // Publish subscription.updated event
    await this.eventBus.publish('user.subscription.updated', {
      userId: user.id,
      subscriptionId: user.subscriptionId,
      plan: user.plan,
      status: user.status,
      nextBillingDate: user.nextBillingDate,
      amount: user.amount,
      previousPlan: user.previousPlan,
      changeReason: subscriptionData.reason,
      metadata: {
        source: 'user-service',
        version: '1.0.0',
        correlationId: subscriptionData.correlationId
      }
    });

    return user;
  }

  async cancelSubscription(userId: string, reason: string) {
    // Cancel subscription in database
    const user = await this.userRepository.cancelSubscription(userId, reason);
    
    // Publish subscription.cancelled event
    await this.eventBus.publish('user.subscription.cancelled', {
      userId: user.id,
      subscriptionId: user.subscriptionId,
      cancelledAt: new Date(),
      reason: reason,
      refundAmount: user.refundAmount,
      metadata: {
        source: 'user-service',
        version: '1.0.0'
      }
    });

    return user;
  }
}
```

### Payment Service Implementation

```typescript
// payment-service/src/payment/payment.service.ts
import { Injectable } from '@nestjs/common';
import { OnEvent, EventPayload } from 'ms-event-stream';

@Injectable()
export class PaymentService {
  constructor(
    private readonly paymentProcessor: PaymentProcessor,
    private readonly invoiceService: InvoiceService,
    private readonly notificationService: NotificationService
  ) {}

  @OnEvent('user.subscription.updated')
  async handleSubscriptionUpdated(event: EventPayload<SubscriptionUpdatedEvent>) {
    const { userId, subscriptionId, plan, status, amount, nextBillingDate } = event.data;
    
    try {
      if (status === 'active') {
        // Process subscription payment
        const payment = await this.paymentProcessor.processSubscriptionPayment({
          userId,
          subscriptionId,
          amount,
          plan,
          nextBillingDate
        });

        // Create invoice
        await this.invoiceService.createInvoice({
          userId,
          paymentId: payment.id,
          amount,
          description: `${plan} subscription payment`,
          dueDate: nextBillingDate
        });

        // Send confirmation
        await this.notificationService.sendPaymentConfirmation(userId, payment);
        
        console.log(`Payment processed for subscription ${subscriptionId}: ${payment.id}`);
      } else if (status === 'cancelled') {
        // Handle subscription cancellation
        await this.handleSubscriptionCancellation(event.data);
      }
    } catch (error) {
      console.error(`Failed to process subscription update for user ${userId}:`, error);
      throw error; // Event will be retried automatically
    }
  }

  @OnEvent('user.subscription.cancelled')
  async handleSubscriptionCancelled(event: EventPayload<SubscriptionCancelledEvent>) {
    const { userId, subscriptionId, reason, refundAmount } = event.data;
    
    try {
      // Process refund if applicable
      if (refundAmount > 0) {
        const refund = await this.paymentProcessor.processRefund({
          userId,
          subscriptionId,
          amount: refundAmount,
          reason: `Subscription cancelled: ${reason}`
        });
        
        console.log(`Refund processed: ${refund.id} for amount ${refundAmount}`);
      }

      // Send cancellation confirmation
      await this.notificationService.sendCancellationConfirmation(userId, {
        subscriptionId,
        reason,
        refundAmount
      });
    } catch (error) {
      console.error(`Failed to handle subscription cancellation for user ${userId}:`, error);
      throw error;
    }
  }

  @OnEvent('user.created')
  async handleUserCreated(event: EventPayload<UserCreatedEvent>) {
    const { userId, email, name } = event.data;
    
    try {
      // Set up default payment methods
      await this.paymentProcessor.setupDefaultPaymentMethods(userId);
      
      // Send welcome email with payment setup
      await this.notificationService.sendWelcomeEmail(email, {
        userId,
        name,
        nextSteps: ['Add payment method', 'Choose subscription plan']
      });
      
      console.log(`Default payment methods set up for user ${userId}`);
    } catch (error) {
      console.error(`Failed to set up payment methods for user ${userId}:`, error);
      throw error;
    }
  }

  private async handleSubscriptionCancellation(data: SubscriptionUpdatedEvent) {
    // Handle downgrade or cancellation logic
    await this.paymentProcessor.handleSubscriptionChange(data);
  }
}
```

### Event Types and Interfaces

```typescript
// shared/types/events.ts
export interface UserCreatedEvent {
  userId: string;
  email: string;
  name: string;
  createdAt: Date;
  metadata: EventMetadata;
}

export interface SubscriptionUpdatedEvent {
  userId: string;
  subscriptionId: string;
  plan: string;
  status: 'active' | 'cancelled' | 'suspended' | 'expired';
  nextBillingDate: Date;
  amount: number;
  previousPlan?: string;
  changeReason?: string;
  metadata: EventMetadata;
}

export interface SubscriptionCancelledEvent {
  userId: string;
  subscriptionId: string;
  cancelledAt: Date;
  reason: string;
  refundAmount: number;
  metadata: EventMetadata;
}

export interface EventMetadata {
  source: string;
  version: string;
  correlationId?: string;
  timestamp: Date;
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

This package is **production-ready** with comprehensive event streaming capabilities:

- ✅ **Phase 1**: Core structure and interfaces
- ✅ **Phase 2**: Database adapter and table creation
- ✅ **Phase 3**: Redis adapter and streams
- ✅ **Phase 4**: Outbox pattern implementation
- ✅ **Phase 5**: Inbox pattern implementation
- ✅ **Phase 6**: Event consumer system
- ✅ **Phase 7**: @OnEvent decorator
- ✅ **Phase 8**: Retry logic & error handling
- ✅ **Phase 9**: Dead Letter Queue (DLQ)
- ✅ **Phase 10**: Event archival system
- ✅ **Phase 11**: Metrics & observability
- ✅ **Phase 12**: Distributed tracing
- ✅ **Phase 13**: Security features
- ✅ **Phase 14**: Advanced scaling features
- ✅ **Phase 15**: CLI tool & developer experience
- ✅ **Phase 16**: Schema validation & versioning
- ✅ **Phase 17**: Point-in-time recovery

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
