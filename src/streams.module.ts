import { DynamicModule, Module, Provider } from "@nestjs/common";
import { StreamBusService } from "./stream-bus.service";

/**
 * Injection token for the IStreamRedis implementation.
 *
 * Each microservice provides its RedisService (which implements IStreamRedis)
 * under this token via StreamsModule.forRoot().
 */
export const STREAM_REDIS = Symbol("STREAM_REDIS");

/**
 * Dynamic module that registers the StreamBusService with the provided
 * IStreamRedis implementation.
 *
 * Each microservice calls forRoot() in its CommonModule (or equivalent):
 *
 *   StreamsModule.forRoot({
 *     streamRedis: { provide: STREAM_REDIS, useExisting: RedisService },
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
}
