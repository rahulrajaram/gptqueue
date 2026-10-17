import { z } from "zod";
import { discoveryPagingShape } from "../../core/agent-discovery.js";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";

export const listAgentsSchema = z.object(discoveryPagingShape).strict();

export const LIST_AGENTS_DESCRIPTION =
  "[safety: readonly] List agents with readable labels, exact messaging names, public UUIDs, working directories, clients, registration times, process IDs, and presence, in name order. " +
  "Bounded: returns at most `limit` agents (default 50, max 500); filter with name_prefix and online_only, and page with the returned next_cursor. Send messages to the full name; labels are for display.";

/**
 * The first text item stays the JSON array of agents (legacy shape). When
 * more agents match, a second text item says so, so text-only callers see it.
 */
export async function listAgents(client: RedisClient, params: z.input<typeof listAgentsSchema> = {}) {
  const paging = listAgentsSchema.parse(params);
  const page = await client.listAgentsPage(paging);
  const note = page.truncated
    ? `Showing ${page.items.length} of ${page.total}; more agents match. Call list_agents again with cursor: ${JSON.stringify(page.next_cursor)}, or narrow with name_prefix or online_only.`
    : undefined;
  const result = toolResult({ status: "ok", agents: page.items, total: page.total, truncated: page.truncated,
    next_cursor: page.next_cursor, ...(note ? { note } : {}) }, false, page.items);
  return note ? { ...result, content: [...result.content, { type: "text" as const, text: note }] } : result;
}
