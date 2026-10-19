/**
 * The unix-socket form of a Redis connection string: an absolute socket path,
 * optionally followed by `?db=N` (N = 0-15), e.g. `/run/redis/redis.sock?db=14`.
 *
 * ioredis already accepts this exact string (its URL parser turns a
 * scheme-less absolute pathname into `path` and the query into `db`), and so
 * does every gptqueue entry point that hands REDIS_URL straight to ioredis.
 * The CLIs that validate `--redis-url` accept it through this parser.
 *
 * The path's character set is restricted to characters no URL parser treats
 * specially, and `//` is refused (a leading `//` would make a legacy parser
 * read a host), so the validated path is exactly the path ioredis connects to.
 */
export type RedisSocketTarget = Readonly<{ path: string; db: number }>;

const PATH = /^\/[A-Za-z0-9._~@+,=:/-]*$/u;
const DB_QUERY = /^db=(0|[1-9]|1[0-5])$/u;
const EXPECTED = "expected an absolute socket path such as /run/redis/redis.sock, optionally followed by ?db=0-15";

const invalid = (reason: string): Error => new Error(`Invalid Redis socket path (${EXPECTED}): ${reason}`);

/**
 * Parse the socket form. Returns undefined when `value` is not a socket form
 * (it does not start with `/`), so the caller's `redis://` handling applies
 * unchanged; throws a descriptive error for a malformed socket form.
 */
export const parseRedisSocketUrl = (value: string): RedisSocketTarget | undefined => {
  if (!value.startsWith("/")) return undefined;
  const mark = value.indexOf("?");
  const path = mark === -1 ? value : value.slice(0, mark);
  const query = mark === -1 ? undefined : value.slice(mark + 1);
  if (!PATH.test(path)) throw invalid("the path contains unsupported characters");
  if (path.includes("//")) throw invalid("the path contains an empty segment (//)");
  if (path.endsWith("/")) throw invalid("the path names a directory, not a socket");
  if (query === undefined) return Object.freeze({ path, db: 0 });
  const db = DB_QUERY.exec(query)?.[1];
  if (db === undefined) throw invalid("the only accepted query is db=N with N in 0-15");
  return Object.freeze({ path, db: Number(db) });
};
