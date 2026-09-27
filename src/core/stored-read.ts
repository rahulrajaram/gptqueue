/** Shared read-side scaffolding for the Redis-backed JSON stores. */

import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/** A short, bounded excerpt of a raw stored value, for store_corrupt diagnostics. */
export const excerptOf = (raw: string): string =>
  raw.length <= 80 ? raw : `${raw.slice(0, 80)}...`;

/** Outcome of reading one stored record: the record (or null when absent), or corrupt. */
export type StoredRead<T> =
  | { readonly kind: "record"; readonly record: T | null }
  | { readonly kind: "corrupt"; readonly excerpt: string };

/** Directory of the Lua scripts the stores load (src/mcp-server/lua, copied to dist). */
export const LUA_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "mcp-server", "lua");
