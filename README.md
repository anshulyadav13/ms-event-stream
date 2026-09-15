# ms-event-stream

Shared Redis Streams publish/consume library for nestys microservices.

Provides standardized stream naming, typed payloads, consumer groups, crash recovery (XAUTOCLAIM), and dead-letter queues (DLQ) — so every microservice communicates the same way without duplicating Redis Streams logic.

For domain-specific integration guides (e.g. the notification microservice), see the consuming service's own README.

---

## Quick Start

### 1. Install

```bash
npm install ms-event-stream
```

### 2. Quick Start with ioredis

If your microservice already has an ioredis client for the event Redis, you do not need to implement `IStreamRedis` yourself. The package provides `IorRedisStreamAdapter` automatically.

```typescript
// src/common/streams/streams.module.ts
import { Global, Module } from '@nestjs/common';
import { StreamsModule } from 'ms-event-stream';

@Global()
@Module({
  imports: [
    StreamsModule.forRootAsync({
      useFactory: (redisService: RedisService) => redisService.getEventClient(),
      inject: [RedisService],
    }),
  ],
  exports: [StreamsModule],
})
export class StreamsModuleWrapper {}
```

If you already have a `Redis` instance at module configuration time:

```typescript
import Redis from 'ioredis';

const redisClient = new Redis({ host: process.env.REDIS_EVENT_HOST });

@Global()
@Module({
  imports: [StreamsModule.forRootIoredis(redisClient)],
  exports: [StreamsModule],
})
export class StreamsModuleWrapper {}
```

`StreamBusService` is then available everywhere without per-module imports.

### 3. Implement `IStreamRedis` in your microservice's `RedisService`

Your `RedisService` must implement the `IStreamRedis` interface (7 methods: `xadd`, `xgroupCreate`, `xreadGroup`, `xack`, `xautoclaim`, `xpending`, `xlen`). All stream operations should use the **event Redis client** (not the cache client).

```typescript
// src/common/redis/redis.service.ts (excerpt)
import { IStreamRedis, StreamEntry } from 'ms-event-stream';

@Injectable()
export class RedisService implements IStreamRedis, OnModuleInit {
  private eventClient: Redis; // ioredis instance for streams

  async xadd(stream: string, fields: Record<string, string>, maxlen?: number): Promise<string> {
    const args = [];
    if (maxlen) args.push('MAXLEN', '~', maxlen);
    args.push('*');
    for (const [k, v] of Object.entries(fields)) args.push(k, v);
    return this.eventClient.xadd(stream, ...args);
  }

  async xgroupCreate(stream: string, group: string, startId: string, mkstream = true): Promise<void> {
    try {
      await this.eventClient.xgroup('CREATE', stream, group, startId, mkstream ? 'MKSTREAM' : '');
    } catch (e) {
      if (!String(e).includes('BUSYGROUP')) throw e; // group already exists — OK
    }
  }

  async xreadGroup(stream, group, consumer, count, block): Promise<StreamEntry[]> { /* ... */ }
  async xack(stream, group, ...ids): Promise<number> { /* ... */ }
  async xautoclaim(stream, group, consumer, minIdle, cursor, count): Promise<{ entries: StreamEntry[]; nextCursor: string }> { /* ... */ }
  async xpending(stream, group, start, end, count): Promise<unknown[]> { /* ... */ }
  async xlen(stream): Promise<number> { /* ... */ }
}
```

See `src/stream-redis.interface.ts` for the full interface.

### 4. Register the module (custom IStreamRedis only)

```typescript
// src/common/streams/streams.module.ts
import { Global, Module } from '@nestjs/common';
import { StreamsModule, STREAM_REDIS } from 'ms-event-stream';
import { RedisService } from '../redis/redis.service';

@Global()
@Module({
  imports: [
    StreamsModule.forRoot({
      streamRedis: { provide: STREAM_REDIS, useExisting: RedisService },
    }),
  ],
  exports: [StreamsModule],
})
export class StreamsModuleWrapper {}
```

Import `StreamsModuleWrapper` in your `AppModule`. Because it is `@Global()`, `StreamBusService` is available everywhere without per-module imports.

---

## Publishing Events

### Recommended: create a typed publisher in your microservice

Each microservice should create a small, typed publisher that wraps `StreamBusService` and exposes methods for the streams it owns. Keep this publisher local to your service; it is not part of `ms-event-stream`.

```typescript
// src/common/services/order-publisher.service.ts
import { Injectable, Logger } from '@nestjs/common';
import { StreamBusService, StreamNames } from 'ms-event-stream';

@Injectable()
export class OrderPublisherService {
  private readonly logger = new Logger(OrderPublisherService.name);

  constructor(private readonly streamBus: StreamBusService) {}

  async orderPlaced(data: { orderId: string; userId: number; total: number }) {
    const names = StreamNames.orderPlaced();
    await this.streamBus.publish(names.stream, {
      orderId: data.orderId,
      userId: data.userId,
      total: data.total,
    });
    this.logger.log(`order.placed published for user ${data.userId}`);
  }
}
```

Register it locally:

```typescript
// src/common/services/order-publisher.module.ts
import { Module } from '@nestjs/common';
import { OrderPublisherService } from './order-publisher.service';

@Module({
  providers: [OrderPublisherService],
  exports: [OrderPublisherService],
})
export class OrderPublisherModule {}
```

### Direct publishing (low-level)

You can also inject `StreamBusService` directly and call `publish()` with a stream name and typed payload:

```typescript
import { Injectable } from '@nestjs/common';
import { StreamBusService, StreamNames } from 'ms-event-stream';

@Injectable()
export class SomeService {
  constructor(private readonly streamBus: StreamBusService) {}

  async sendSomething(userId: number) {
    const names = StreamNames.someEvent();
    await this.streamBus.publish(names.stream, {
      userId,
      event: 'some.action',
      idempotencyKey: `some:${userId}:${Date.now()}`,
    });
  }
}
```

### Serialization

Payloads are serialized automatically to flat string fields for XADD:
- Primitives (string, number, boolean) → `String(value)`
- Objects/arrays → `JSON.stringify`
- `undefined`/`null` fields → omitted

---

## Consuming Events

Create a consumer service that implements `OnModuleInit` and `OnModuleDestroy`:

```typescript
import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import {
  StreamBusService,
  StreamNames,
  StreamConsumerHandle,
} from 'ms-event-stream';

@Injectable()
export class OrderPlacedConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(OrderPlacedConsumer.name);
  private handle: StreamConsumerHandle | null = null;

  constructor(private readonly streamBus: StreamBusService) {}

  async onModuleInit() {
    const names = StreamNames.orderPlaced();
    this.handle = await this.streamBus.startConsumer({
      stream: names.stream,
      group: names.group,
      dlq: names.dlq,
      handler: async (payload, entryId) => {
        this.logger.log(`Received order for user ${payload.userId}`);
        // application logic here
      },
    });
  }

  onModuleDestroy() {
    this.handle?.stop();
  }
}
```

Register the consumer in your module's `providers` array. The consumer group, polling, ACK, reclaim sweep, and DLQ are handled automatically.

### Consumer options

| Option | Default | Description |
|---|---|---|
| `stream` | — | Redis Stream key (required) |
| `group` | — | Consumer group name (required) |
| `dlq` | — | Dead-letter queue stream key |
| `handler` | — | Async function called for each entry (required) |
| `consumerId` | `randomUUID()` | Unique consumer identifier |
| `pollIntervalMs` | `1000` | How often to poll for new messages |
| `batchSize` | `20` | Max entries per poll batch |
| `blockMs` | `2000` | Long-poll block duration |
| `reclaimIntervalMs` | `30000` | How often to run the reclaim sweep |
| `minIdleMs` | `60000` | Min idle time before a message is considered stuck |
| `maxDeliveryCount` | `5` | Max attempts before moving to the DLQ |

---

## A Service Can Be Both Publisher and Consumer

A single microservice can publish and consume simultaneously. Just call `startConsumer()` in `onModuleInit()` for each stream you want to consume, and `publish()` wherever you need to send.

```typescript
@Injectable()
export class OrderStreamOrchestrator implements OnModuleInit, OnModuleDestroy {
  private orderHandle: StreamConsumerHandle | null = null;

  constructor(private readonly streamBus: StreamBusService) {}

  async onModuleInit() {
    const names = StreamNames.orderPlaced();
    this.orderHandle = await this.streamBus.startConsumer({
      stream: names.stream,
      group: names.group,
      dlq: names.dlq,
      handler: async (payload) => {
        // handle order placed event
      },
    });
  }

  async publishOrderPlaced(orderId: string, userId: number, total: number) {
    const names = StreamNames.orderPlaced();
    await this.streamBus.publish(names.stream, { orderId, userId, total });
  }

  onModuleDestroy() {
    this.orderHandle?.stop();
  }
}
```

---

## Adding a New Stream

### Step 1 — Add stream names in `src/stream-names.ts`

```typescript
static orderPlaced(): StreamNameSet {
  return {
    stream: 'order:placed:stream',
    group: 'order-placed-workers',
    dlq: 'order:placed:dlq',
  };
}
```

### Step 2 — Add a typed payload in `src/stream-payloads.ts`

```typescript
export interface OrderPlacedPayload {
  orderId: string;
  userId: number;
  total: number;
}
```

### Step 3 — Export from `src/index.ts`

```typescript
export { OrderPlacedPayload } from './stream-payloads';
```

### Step 4 — Rebuild

```bash
cd shared/stream-bus && npm run build
```

All microservices with the package linked will pick up the changes via hot reload.

---

## Stream Naming Standard

```
Stream:          {consumer-service}:{resource}:{action}:stream
Consumer group:  {consumer-service}-{resource}-{action}-workers
DLQ:             {consumer-service}:{resource}:{action}:dlq
```

- `consumer-service` — the microservice that consumes (e.g. `notification`, `order`)
- `resource` — the domain resource (e.g. `dispatch`, `placed`)
- `action` — what's being done (e.g. `register`, `placed`)

---

## Error Handling

The handler is called **without a try/catch** — intentionally:

1. If the handler throws, the message is **NOT ACK'd** — it stays in the PEL
2. The error propagates to the process-level global exception handler
3. The reclaim sweep retries the message after `minIdleMs`
4. After `maxDeliveryCount` attempts, the message is moved to the DLQ

Inspect the DLQ:

```bash
redis-cli XRANGE order:placed:dlq - +
```

---

## Local Development with Docker

The `ms-event-stream` package is installed from npm inside each service container. No shared-package volume is needed.

---

## License

Proprietary — © nestys. All rights reserved.
