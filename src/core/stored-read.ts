/** Shared read-side scaffolding for the Redis-backed JSON stores. */

import { createHash } from "node:crypto";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Non-content diagnostics for a corrupt stored value, for store_corrupt
 * messages. A stored record can hold a session id, which is a bearer
 * credential, so the message carries the value's byte length, a short hash
 * to match it against the stored bytes, and the failing field when the reader
 * knows it, never the bytes themselves.
 */
export const describeStored = (raw: string, failing?: string): string =>
  [
    `${Buffer.byteLength(raw, "utf8")} bytes`,
    `sha256:${createHash("sha256").update(raw).digest("hex").slice(0, 12)}`,
    ...(failing === undefined ? [] : [`failing: ${failing}`]),
  ].join(", ");

/**
 * The first of `fields` that is not a string in a stored JSON object; "JSON"
 * when the value does not parse, "(root)" when it is not an object, and
 * undefined when every listed field is a string.
 */
export const firstNonStringField = (
  raw: string,
  fields: readonly string[]
): string | undefined => {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return "JSON";
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) return "(root)";
  return fields.find((field) => typeof (parsed as Record<string, unknown>)[field] !== "string");
};

/** Outcome of reading one stored record: the record (or null when absent), or corrupt. */
export type StoredRead<T> =
  | { readonly kind: "record"; readonly record: T | null }
  | { readonly kind: "corrupt"; readonly diagnostic: string };

/** Directory of the Lua scripts the stores load (src/mcp-server/lua, copied to dist). */
export const LUA_DIR = join(dirname(fileURLToPath(import.meta.url)), "..", "mcp-server", "lua");
