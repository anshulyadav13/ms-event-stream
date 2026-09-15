import { DynamicModule, Module, Provider } from "@nestjs/common";
import Redis from "ioredis";
import { StreamBusService } from "./stream-bus.service";
import { IorRedisStreamAdapter } from "./ioredis-stream-adapter";

type RedisClientOrFactory = Redis | (() => Redis);

/**
 * Injection token for the IStreamRedis implementation.
 *
 * Each microservice provides its RedisService (which implements IStreamRedis)
 * under this token via StreamsModule.forRoot(), or the package provides an
 * IorRedisStreamAdapter automatically via forRootIoredis()/forRootAsync().
 */
export const STREAM_REDIS = Symbol("STREAM_REDIS");

/**
 * Dynamic module that registers the StreamBusService with the provided
 * IStreamRedis implementation.
 *
 * Option A — bring your own IStreamRedis:
 *
 *   StreamsModule.forRoot({
 *     streamRedis: { provide: STREAM_REDIS, useExisting: RedisService },
 *   })
 *
 * Option B — let the package wrap an ioredis client:
 *
 *   StreamsModule.forRootIoredis(redisClient)
 *
 * Option C — async factory, e.g. read the event client from your RedisService:
 *
 *   StreamsModule.forRootAsync({
 *     useFactory: (redisService: RedisService) => redisService.getEventClient(),
 *     inject: [RedisService],
 *   })
 *
 * This makes StreamBusService available for injection throughout the MS.
 */
@Module({})
export class StreamsModule {
  static forRoot(options: {
    streamRedis: Provider;
  }): DynamicModule {
    return {
      module: StreamsModule,
      providers: [
        options.streamRedis,
        {
          provide: StreamBusService,
          useFactory: (redis: unknown) =>
            new StreamBusService(
              redis as import("./stream-redis.interface").IStreamRedis,
            ),
          inject: [STREAM_REDIS],
        },
      ],
      exports: [StreamBusService],
    };
  }

  /**
   * Convenience factory for services that already have an ioredis client.
   * Pass the event Redis client directly, or a factory function that returns
   * the client lazily.
   */
  static forRootIoredis(client: RedisClientOrFactory): DynamicModule {
    return this.forRoot({
      streamRedis: {
        provide: STREAM_REDIS,
        useValue: new IorRedisStreamAdapter(client),
      },
    });
  }

  /**
   * Async factory. Useful when the ioredis client is only available via
   * another service (e.g. `redisService.getEventClient()`).
   *
   * useFactory may return the client directly, or a function that returns the
   * client lazily. The lazy form is needed when the client is not ready at
   * module initialization time (e.g. it connects in `onModuleInit`).
   */
  static forRootAsync(options: {
    useFactory: (
      ...args: any[]
    ) =>
      | Promise<RedisClientOrFactory>
      | RedisClientOrFactory;
    inject?: any[];
  }): DynamicModule {
    return {
      module: StreamsModule,
      providers: [
        {
          provide: STREAM_REDIS,
          useFactory: async (...args: any[]) => {
            const resolved = await options.useFactory(...args);
            return new IorRedisStreamAdapter(resolved);
          },
          inject: options.inject,
        },
        {
          provide: StreamBusService,
          useFactory: (
            redis: import("./stream-redis.interface").IStreamRedis,
          ) => new StreamBusService(redis),
          inject: [STREAM_REDIS],
        },
      ],
      exports: [StreamBusService],
    };
  }
}
