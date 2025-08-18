/**
 * Example usage of ms-event-stream package
 * This demonstrates the basic API that will be available once all phases are implemented
 */

import { Module, Injectable } from '@nestjs/common';
import { EventStreamModule, EventStreamService, DatabaseAdapter } from './src';

// 1. Configure the module in your app.module.ts
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

// 2. Publish events in your services
@Injectable()
export class OrderService {
  constructor(private readonly eventBus: EventStreamService) {}

  async createOrder(orderData: any) {
    // Your business logic
    const order = await this.saveOrder(orderData);

    // Publish event with outbox pattern (Phase 4)
    await this.eventBus.publish('order.created', {
      orderId: order.id,
      customerId: order.customerId,
      amount: order.amount,
      items: order.items
    });

    return order;
  }

  private async saveOrder(orderData: any): Promise<any> {
    // Mock implementation
    return {
      id: 12345,
      customerId: orderData.customerId,
      amount: orderData.amount,
      items: orderData.items,
      createdAt: new Date()
    };
  }

  /**
   * Phase 2 feature: Check database health
   */
  async checkSystemHealth() {
    const dbHealth = await this.eventBus.getDatabaseHealth();
    const dbStats = this.eventBus.getDatabaseStats();
    
    return {
      database: dbHealth,
      connectionPool: dbStats,
    };
  }
}

// 3. Handle events with decorators (Phase 7)
@Injectable()
export class PaymentService {
  constructor(private readonly eventBus: EventStreamService) {}
  // Note: @OnEvent decorator will be implemented in Phase 7
  // @OnEvent('order.created')
  async handleOrderCreated(event: any) {
    console.log('Processing payment for order:', event.data.orderId);
    
    // Your event handling logic
    await this.processPayment(event.data);
  }

  // Note: @OnEvent decorator will be implemented in Phase 7
  // @OnEvent('payment.failed')
  async handlePaymentFailed(event: any) {
    console.log('Payment failed for order:', event.data.orderId);
    
    // Handle payment failure
    await this.handleFailedPayment(event.data);
  }

  private async processPayment(orderData: any): Promise<void> {
    // Mock payment processing
    console.log(`Processing payment of $${orderData.amount} for order ${orderData.orderId}`);
  }

  private async handleFailedPayment(orderData: any): Promise<void> {
    // Mock failure handling
    console.log(`Handling failed payment for order ${orderData.orderId}`);
  }

  /**
   * Phase 2 feature: Direct database access for advanced use cases
   */
  async getEventHistory(eventType: string) {
    const dbAdapter = this.eventBus.getDatabaseAdapter();
    const result = await dbAdapter.query(`
      SELECT * FROM outbox_events 
      WHERE event_type = $1 
      ORDER BY created_at DESC 
      LIMIT 10
    `, [eventType]);

    return result.data;
  }
}

// 4. Async configuration example
@Module({
  imports: [
    EventStreamModule.forRootAsync({
      useFactory: async (configService: any) => ({
        serviceName: configService.get('SERVICE_NAME'),
        dbUrl: configService.get('DATABASE_URL'),
        redisUrl: configService.get('REDIS_URL'),
        env: configService.get('NODE_ENV'),
        
        // Advanced configuration
        database: {
          maxConnections: 20,
          connectionTimeoutMs: 5000,
        },
        redis: {
          retryAttempts: 5,
          retryDelayMs: 2000,
        },
        events: {
          maxConcurrentEvents: 100,
          enableDeduplication: true,
        }
      }),
      inject: ['ConfigService'],
    })
  ],
})
export class AsyncConfigModule {}
