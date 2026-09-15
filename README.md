# ms-event-stream

Shared Redis Streams publish/consume library for nestys microservices.

Provides standardized stream naming, typed payloads, consumer groups, crash recovery (XAUTOCLAIM), and dead-letter queues (DLQ) — so every microservice communicates the same way without duplicating Redis Streams logic.

---

## Quick Start

### 1. Install

```bash
# Local development (file link)
npm install file:../shared/stream-bus

# After install, build the package:
cd ../shared/stream-bus && npm run build
```

### 2. Implement `IStreamRedis` in your microservice's `RedisService`

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
      await this.eventClient.xgroup('CREATE', stream, group, startId, mkstack ? 'MKSTREAM' : '');
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

### 3. Register the module

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

Import `StreamsModuleWrapper` in your `AppModule`. Because it's `@Global()`, `StreamBusService` is available everywhere without per-module imports.

---

## Publishing Events

### Recommended: use `NotificationPublisher` in your microservice

Each microservice that publishes notifications should have a dedicated `NotificationPublisher` service (separate from the generic `ServiceIntegrationService` HTTP client). It injects `StreamBusService` and exposes typed methods for push, email, and FCM token lifecycle:

```typescript
// src/common/services/notification-publisher.service.ts
import { Injectable, Logger } from '@nestjs/common';
import {
  StreamBusService,
  StreamNames,
  DeviceTokenRegisterPayload,
  DeviceTokenRemovePayload,
  NotificationDispatchPayload,
} from 'ms-event-stream';

@Injectable()
export class NotificationPublisher {
  private readonly logger = new Logger(NotificationPublisher.name);

  constructor(private readonly streamBus: StreamBusService) {}

  /** Send a push notification (channel = PUSH) */
  async sendPushNotification(params: {
    userId: number;
    templateId: string;
    idempotencyKey: string;
    locale?: string;
    templateParams?: Record<string, unknown>;
  }): Promise<boolean> {
    const names = StreamNames.notificationDispatch();
    await this.streamBus.publish(names.stream, {
      userId: params.userId,
      channel: 'PUSH',
      idempotencyKey: params.idempotencyKey,
      templateId: params.templateId,
      locale: params.locale || 'en',
      params: params.templateParams,
    } as NotificationDispatchPayload);
    return true;
  }

  /** Send an email notification (channel = EMAIL) */
  async sendEmailNotification(params: {
    userId: number;
    toEmail: string;
    templateId: string;
    idempotencyKey: string;
    locale?: string;
    templateParams?: Record<string, unknown>;
  }): Promise<boolean> {
    const names = StreamNames.notificationDispatch();
    await this.streamBus.publish(names.stream, {
      userId: params.userId,
      channel: 'EMAIL',
      idempotencyKey: params.idempotencyKey,
      toEmail: params.toEmail,
      templateId: params.templateId,
      locale: params.locale || 'en',
      params: params.templateParams,
    } as NotificationDispatchPayload);
    return true;
  }

  /** Register an FCM device token (called on login) */
  async registerFcmToken(userId: number, token: string, deviceId: string, platform: string) {
    const names = StreamNames.notificationDeviceTokenRegister();
    await this.streamBus.publish(names.stream, { userId, token, deviceId, platform } as DeviceTokenRegisterPayload);
  }

  /** Remove an FCM device token (called on logout) */
  async removeFcmToken(userId: number, token: string) {
    const names = StreamNames.notificationDeviceTokenRemove();
    await this.streamBus.publish(names.stream, { userId, token } as DeviceTokenRemovePayload);
  }
}
```

Register it in a module:

```typescript
// src/common/services/notification-publisher.module.ts
import { Module } from '@nestjs/common';
import { NotificationPublisher } from './notification-publisher.service';

@Module({
  providers: [NotificationPublisher],
  exports: [NotificationPublisher],
})
export class NotificationPublisherModule {}
```

`StreamBusService` is registered globally by `StreamsModuleWrapper`, so no explicit import is needed in the module.

### Direct publishing (low-level)

You can also inject `StreamBusService` directly and call `publish()` with a stream name and typed payload:

```typescript
import { Injectable } from '@nestjs/common';
import { StreamBusService, StreamNames } from 'ms-event-stream';

@Injectable()
export class SomeService {
  constructor(private readonly streamBus: StreamBusService) {}

  async sendNotification(userId: number) {
    const names = StreamNames.notificationDispatch();
    await this.streamBus.publish(names.stream, {
      userId,
      channel: 'PUSH',
      idempotencyKey: `order-${userId}-${Date.now()}`,
      templateId: 'order.confirmed.push',
      locale: 'en',
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
  NotificationDispatchPayload,
} from 'ms-event-stream';

@Injectable()
export class NotificationDispatchConsumer implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(NotificationDispatchConsumer.name);
  private handle: StreamConsumerHandle | null = null;

  constructor(
    private readonly streamBus: StreamBusService,
    private readonly dispatchService: DispatchService,
  ) {}

  async onModuleInit() {
    const names = StreamNames.notificationDispatch();
    this.handle = await this.streamBus.startConsumer<NotificationDispatchPayload>({
      stream: names.stream,
      group: names.group,
      dlq: names.dlq,
      handler: async (payload, entryId) => {
        this.logger.log(`Received dispatch for user ${payload.userId}`);
        await this.dispatchService.dispatch({
          userId: Number(payload.userId),
          channel: payload.channel,
          idempotencyKey: payload.idempotencyKey,
          title: payload.title,
          body: payload.body,
        });
      },
    });
  }

  onModuleDestroy() {
    this.handle?.stop();
  }
}
```

Register the consumer in your module's `providers` array. The consumer group, polling, ACK, reclaim sweep, and DLQ are all handled automatically.

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
| `maxDeliveryCount` | `5` | Max attempts before moving to DLQ |

---

## A Service Can Be Both Publisher and Consumer

A single microservice can publish and consume simultaneously. Just call `startConsumer()` in `onModuleInit()` for each stream you want to consume, and `publish()` wherever you need to send.

```typescript
@Injectable()
export class AuthStreamOrchestrator implements OnModuleInit, OnModuleDestroy {
  private profileHandle: StreamConsumerHandle | null = null;

  constructor(private readonly streamBus: StreamBusService) {}

  async onModuleInit() {
    // Consumer: listen for user profile updates
    const names = StreamNames.userProfileUpdated();
    this.profileHandle = await this.streamBus.startConsumer<UserProfileUpdatedPayload>({
      stream: names.stream,
      group: names.group,
      dlq: names.dlq,
      handler: async (payload) => {
        await this.cacheService.invalidate(`user:${payload.userId}`);
      },
    });
  }

  // Publisher: called from auth.service.ts on login
  async publishFcmToken(userId: number, token: string, deviceId: string, platform: string) {
    const names = StreamNames.notificationDeviceTokenRegister();
    await this.streamBus.publish(names.stream, { userId, token, deviceId, platform });
  }

  onModuleDestroy() {
    this.profileHandle?.stop();
  }
}
```

---

## Available Streams

| Stream | Group | Publisher | Consumer | Payload |
|---|---|---|---|---|
| `notification:dispatch:stream` | `notification-workers` | auth MS, user MS, any MS | notification MS | `NotificationDispatchPayload` |
| `notification:device-token:register:stream` | `notification-device-token-register-workers` | auth MS, user MS | notification MS | `DeviceTokenRegisterPayload` |
| `notification:device-token:remove:stream` | `notification-device-token-remove-workers` | auth MS, user MS | notification MS | `DeviceTokenRemovePayload` |

---

## Adding a New Stream

### Step 1 — Add stream names in `src/stream-names.ts`

```typescript
static userProfileUpdated(): StreamNameSet {
  return {
    stream: 'user:profile-updated:stream',
    group: 'user-profile-updated-workers',
    dlq: 'user:profile-updated:dlq',
  };
}
```

### Step 2 — Add a typed payload in `src/stream-payloads.ts`

```typescript
export interface UserProfileUpdatedPayload {
  userId: number;
  email?: string;
  username?: string;
  updatedAt: string;
}
```

### Step 3 — Export from `src/index.ts`

```typescript
export { UserProfileUpdatedPayload } from './stream-payloads';
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

- `consumer-service` — the microservice that consumes (e.g. `notification`)
- `resource` — the domain resource (e.g. `dispatch`, `device-token`)
- `action` — what's being done (e.g. `register`, `remove`)

---

## Error Handling

The handler is called **without a try/catch** — intentionally:

1. If the handler throws, the message is **NOT ACK'd** — it stays in the PEL
2. The error propagates to the process-level global exception handler
3. The reclaim sweep retries the message after `minIdleMs`
4. After `maxDeliveryCount` attempts, the message is moved to the DLQ

Inspect the DLQ:

```bash
redis-cli XRANGE notification:dispatch:dlq - +
```

---

## Local Development with Docker

The shared package is mounted as a volume so all services pick up changes instantly:

```yaml
# docker-compose.dev.yml
volumes:
  - ./src:/app/src
  - /app/node_modules
  - ../shared/stream-bus:/app/node_modules/ms-event-stream
```

After editing the shared package, rebuild it:

```bash
cd shared/stream-bus && npm run build
```

All services will pick up the changes on their next file-watcher cycle — no container restart needed.

---

## License

Proprietary — © nestys. All rights reserved.
