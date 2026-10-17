import { z } from "zod";

/** Public identity metadata stored alongside a queue registration. */
export type AgentDiscoveryMetadata = Readonly<{
  label: string;
  uuid: string | null;
  client: "codex" | "pi" | "opencode" | null;
  working_directory: string | null;
}>;

export type AgentDiscoveryRecord = Readonly<AgentDiscoveryMetadata & {
  name: string;
  role: string;
  description?: string;
  online: boolean;
  registered_at: string | null;
  pid: number | null;
}>;

const nonblank = (value: unknown): string | null =>
  typeof value === "string" && value.trim().length > 0 ? value.trim() : null;

const uuid = (value: unknown): string | null =>
  typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value)
    ? value : null;

const client = (value: unknown): "codex" | "pi" | "opencode" | null =>
  value === "codex" || value === "pi" || value === "opencode" ? value : null;

const directory = (value: unknown): string | null =>
  typeof value === "string" && value.startsWith("/") ? value : null;

const discoveryMetadata = (
  name: string,
  metadata?: unknown
): AgentDiscoveryMetadata => {
  const values = metadata && typeof metadata === "object" && !Array.isArray(metadata)
    ? metadata as Record<string, unknown> : {};
  return Object.freeze({
    label: nonblank(values.label) ?? name,
    uuid: uuid(values.uuid),
    client: client(values.client),
    working_directory: directory(values.working_directory),
  });
};

export const discoveryRecord = (input: {
  name: string; role: string; description?: string; online: boolean;
  registered_at?: unknown; pid?: unknown; metadata?: unknown;
}): AgentDiscoveryRecord => Object.freeze({
  name: input.name,
  role: input.role,
  description: input.description,
  online: input.online,
  ...discoveryMetadata(input.name, input.metadata),
  registered_at: typeof input.registered_at === "string" && !Number.isNaN(Date.parse(input.registered_at)) ? input.registered_at : null,
  pid: typeof input.pid === "number" && Number.isSafeInteger(input.pid) && input.pid > 0 ? input.pid : null,
});

/** Discovery is bounded by default: a registry can hold hundreds of agents. */
export const DISCOVERY_DEFAULT_LIMIT = 50;
export const DISCOVERY_MAX_LIMIT = 500;

/**
 * Shared optional filters and paging for list_agents and find_agents. Every
 * field is optional, so a caller passing nothing gets the first bounded page.
 */
export const discoveryPagingShape = {
  name_prefix: z.string().max(500).optional()
    .describe("Only agents whose exact messaging name starts with this prefix."),
  online_only: z.boolean().optional().describe("Only agents currently online."),
  limit: z.number().int().min(1).max(DISCOVERY_MAX_LIMIT).default(DISCOVERY_DEFAULT_LIMIT)
    .describe(`Page size (default ${DISCOVERY_DEFAULT_LIMIT}, max ${DISCOVERY_MAX_LIMIT}).`),
  cursor: z.string().regex(/^\d{1,9}$/u).optional()
    .describe("next_cursor from a previous page; takes precedence over offset."),
  offset: z.number().int().min(0).max(999_999_999).optional()
    .describe("Zero-based start position in name order (numeric form of cursor)."),
} as const;

export type DiscoveryPaging = Readonly<{
  name_prefix?: string; online_only?: boolean; limit?: number; cursor?: string; offset?: number;
}>;

/** Start position of the requested page. */
export const pageStart = (paging: DiscoveryPaging): number =>
  paging.cursor !== undefined ? Number(paging.cursor) : paging.offset ?? 0;

export type DiscoveryPage<T> = Readonly<{
  items: readonly T[]; total: number; truncated: boolean; next_cursor: string | null;
}>;

/** One page of an already filtered, stably ordered list. `truncated` means more follow this page. */
export const discoveryPage = <T>(items: readonly T[], paging: DiscoveryPaging): DiscoveryPage<T> => {
  const start = pageStart(paging);
  const end = start + Math.min(DISCOVERY_MAX_LIMIT, Math.max(1, Math.floor(paging.limit ?? DISCOVERY_DEFAULT_LIMIT)));
  const more = end < items.length;
  return Object.freeze({ items: Object.freeze(items.slice(start, end)), total: items.length,
    truncated: more, next_cursor: more ? String(end) : null });
};

/** Registry names in stable paging order, after the cheap name filter. */
export const discoveryNames = (names: readonly string[], namePrefix?: string): string[] =>
  names.filter((name) => !namePrefix || name.startsWith(namePrefix)).sort();
