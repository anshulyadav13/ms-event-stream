import Redis from "ioredis";
import { IStreamRedis, StreamEntry } from "./stream-redis.interface";

type RedisClientOrFactory = Redis | (() => Redis);

/**
 * Default IStreamRedis adapter for ioredis.
 *
 * This makes the `ms-event-stream` package usable out of the box: a producer
 * or consumer just needs to provide its ioredis event client. No custom
 * `IStreamRedis` implementation is required.
 *
 * Accepts either a Redis client directly or a factory function so the client
 * can be resolved lazily (useful when the client is not ready at module
 * initialization time, e.g. a NestJS RedisService that connects in onModuleInit).
 */
export class IorRedisStreamAdapter implements IStreamRedis {
  constructor(private readonly clientOrFactory: RedisClientOrFactory) {}

  private getClient(): Redis {
    if (typeof this.clientOrFactory === "function") {
      return this.clientOrFactory();
    }
    return this.clientOrFactory;
  }

  async xadd(
    stream: string,
    fields: Record<string, string>,
    maxlen?: number,
  ): Promise<string> {
    const args: (string | number)[] = [];
    if (maxlen !== undefined) {
      args.push("MAXLEN", "~", String(maxlen));
    }
    args.push("*");
    for (const [k, v] of Object.entries(fields)) {
      args.push(k, v);
    }
    return this.getClient().xadd(stream, ...args) as Promise<string>;
  }

  async xgroupCreate(
    stream: string,
    group: string,
    startId: string,
    mkstream = true,
  ): Promise<void> {
    try {
      const args = ["CREATE", stream, group, startId];
      if (mkstream) args.push("MKSTREAM");
      await (this.getClient() as any).xgroup(...args);
    } catch (error) {
      if (!String(error).includes("BUSYGROUP")) {
        throw error;
      }
    }
  }

  async xreadGroup(
    stream: string,
    group: string,
    consumer: string,
    count: number,
    blockMs: number,
  ): Promise<StreamEntry[]> {
    const reply = (await this.getClient().xreadgroup(
      "GROUP",
      group,
      consumer,
      "COUNT",
      count,
      "BLOCK",
      blockMs,
      "STREAMS",
      stream,
      ">",
    )) as Array<[string, Array<[string, string[]] | null>]> | null;

    if (!reply || reply.length === 0) {
      return [];
    }

    const result: StreamEntry[] = [];
    for (const [, messages] of reply) {
      if (!messages) continue;
      for (const msg of messages) {
        if (!msg) continue;
        const [id, flatFields] = msg;
        const fields: Record<string, string> = {};
        for (let i = 0; i < flatFields.length; i += 2) {
          fields[flatFields[i]] = flatFields[i + 1];
        }
        result.push({ id, fields });
      }
    }
    return result;
  }

  async xack(stream: string, group: string, ...ids: string[]): Promise<number> {
    return this.getClient().xack(stream, group, ...ids);
  }

  async xautoclaim(
    stream: string,
    group: string,
    consumer: string,
    minIdleMs: number,
    startId: string,
    count: number,
  ): Promise<{ nextCursor: string; entries: StreamEntry[] }> {
    const reply = (await this.getClient().xautoclaim(
      stream,
      group,
      consumer,
      minIdleMs,
      startId,
      "COUNT",
      count,
    )) as [string, Array<[string, string[]] | null>];

    const [nextCursor, messages] = reply;
    const entries: StreamEntry[] = [];
    for (const msg of messages) {
      if (!msg) continue;
      const [id, flatFields] = msg;
      const fields: Record<string, string> = {};
      for (let i = 0; i < flatFields.length; i += 2) {
        fields[flatFields[i]] = flatFields[i + 1];
      }
      entries.push({ id, fields });
    }

    return { nextCursor, entries };
  }

  async xpending(
    stream: string,
    group: string,
    start: string,
    end: string,
    count: number,
  ): Promise<Array<[string, string, number, number]>> {
    return this.getClient().xpending(stream, group, start, end, count) as Promise<
      Array<[string, string, number, number]>
    >;
  }

  async xlen(stream: string): Promise<number> {
    return this.getClient().xlen(stream);
  }
}
