import { z } from "zod";
import type { RedisClient } from "../redis-client.js";
import { toolResult } from "../tool-result.js";
import type { CustodyRecord } from "../../core/custody-model.js";
import { sessionTag } from "../../core/session-tag.js";

export const custodyStatusSchema = z.object({
  worktree_path: z
    .string()
    .min(1)
    .optional()
    .describe(
      "Absolute path of one worktree to inspect; omit to list every stored custody record"
    ),
});

/**
 * Public projection. custody_status is readable by any participant, even
 * before registration, and a held record names its custodian's session, a
 * bearer credential, so it becomes its tag; field names and shape stay.
 * custody_claim and custody_release return the caller's own record raw.
 */
const publicCustody = (record: CustodyRecord): CustodyRecord =>
  record.custodian === undefined
    ? record
    : Object.freeze({
        ...record,
        custodian: Object.freeze({
          ...record.custodian,
          session_id: sessionTag(record.custodian.session_id),
        }),
      });

export async function custodyStatus(
  client: RedisClient,
  params: z.infer<typeof custodyStatusSchema>
) {
  const result = await client.custody.status({
    worktree_path: params.worktree_path,
    now: new Date().toISOString(),
  });

  if (!result.ok) {
    return toolResult(
      {
        status: "error",
        error: { code: result.error.code, message: result.error.message },
      },
      true
    );
  }
  return "record" in result
    ? toolResult({ status: "ok", record: result.record === null ? null : publicCustody(result.record) })
    : toolResult({ status: "ok", records: result.records.map(publicCustody) });
}