import { parseRedisSocketUrl } from "../core/redis-socket-url.js";

export type ShellClient = "codex" | "pi";
export type RegisteredShellArgs = Readonly<{ client: ShellClient; redisUrl: string }>;

const parsePairs = (
  tokens: readonly string[],
  values: Readonly<Record<string, string>> = {}
): Readonly<Record<string, string>> => {
  if (tokens.length === 0) return values;
  const [key, value, ...rest] = tokens;
  if ((key !== "--client" && key !== "--redis-url") || !value || value.startsWith("--")) {
    throw new Error("Expected --client and --redis-url option/value pairs");
  }
  if (key in values) throw new Error("Duplicate registered-shell option");
  return parsePairs(rest, { ...values, [key]: value });
};

/** Native option validation shared by startup and read-only process discovery. */
export const parseRegisteredShellOptions = (argv: readonly string[]): RegisteredShellArgs => {
  const values = parsePairs(argv);
  const client = values["--client"];
  const redisUrl = values["--redis-url"];
  if (client !== "codex" && client !== "pi") throw new Error("--client must be codex or pi");
  if (!redisUrl) throw new Error("--redis-url is required and must be explicit");
  // Unix-socket form (/abs/path.sock[?db=N]); throws its own descriptive error.
  if (parseRedisSocketUrl(redisUrl)) return Object.freeze({ client, redisUrl });
  try {
    const parsed = new URL(redisUrl);
    if (!['redis:', 'rediss:'].includes(parsed.protocol) || !parsed.hostname ||
        parsed.search || parsed.hash || !/^\/(?:[0-9]+)?$/u.test(parsed.pathname || "/")) {
      throw new Error("invalid");
    }
  } catch { throw new Error("Invalid Redis URL (expected redis://host/database)"); }
  return Object.freeze({ client, redisUrl });
};
