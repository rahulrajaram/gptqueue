/** Types for scripts/lib/owned-redis.mjs (the one private test Redis implementation). */

export declare const RUN_DIR_PREFIX: string;

export interface PrivateRedisTarget {
  /** Absolute unix-socket path: <runDir>/redis.sock. */
  readonly path: string;
  /** The 0700 run directory holding the socket. */
  readonly dir: string;
  /** Logical database inside the private instance (1-15). */
  readonly db: number;
}

/** Shape check only: `<.../gptq-redis-XXXXXX>/redis.sock?db=1..15`; throws on anything else (any TCP URL). */
export declare function parsePrivateRedisUrl(value: string | undefined): PrivateRedisTarget;

/** Shape check plus live ownership (0700 run dir and socket owned by this user); returns the value. */
export declare function privateRedisUrl(value: string | undefined): string;

export interface ServerFacts {
  readonly port: string;
  readonly unixsocket: string;
  readonly processId: number;
}

export type ServerProbe = (target: { socket: string; timeoutMs: number }) => Promise<ServerFacts>;

export declare const probeServer: ServerProbe;

export declare function isRedisServingSocket(pid: number, socket: string): boolean;

export interface ReapOptions {
  root?: string;
  now?: number;
  foreignStaleMs?: number;
  stopTimeoutMs?: number;
}

export declare function reapStaleOwnedRedis(options?: ReapOptions): Promise<Array<{ dir: string; killed: number | null }>>;

export interface StartOwnedRedisOptions {
  /** Database encoded in `url` (default 15). */
  db?: number;
  /** Parent of the run directory (default os.tmpdir(), i.e. $TMPDIR). */
  root?: string;
  /** redis-server binary (default $GPTQ_TEST_REDIS_SERVER, then /usr/bin/redis-server). */
  serverBin?: string;
  readyTimeoutMs?: number;
  stopTimeoutMs?: number;
  /** Privacy probe; injectable so a forced mismatch can be tested. */
  probe?: ServerProbe;
  /** Reap stale instances under `root` first (default true). */
  reap?: boolean;
}

export interface OwnedRedis {
  /** `<runDir>/redis.sock?db=N`, accepted by ioredis and the gptqueue CLIs. */
  readonly url: string;
  readonly socket: string;
  readonly dir: string;
  readonly db: number;
  readonly pid: number;
  /** SIGTERM, SIGKILL after a bound, then remove the run dir. Idempotent. */
  close(): Promise<void>;
  /** Synchronous variant for exit/signal paths: SIGKILL and remove the run dir. */
  stop(): void;
}

export declare function startOwnedRedis(options?: StartOwnedRedisOptions): Promise<OwnedRedis>;
