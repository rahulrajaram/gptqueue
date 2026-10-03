/** Non-reversible tags for session ids, which are bearer credentials. */

import { createHash } from "node:crypto";

/**
 * A session id authorizes its holder to act as that session's agent, so logs
 * and public reads carry only this short, non-reversible tag: stable for
 * correlation, useless as a credential.
 */
export const sessionTag = (sid: string): string =>
  `sid-sha256:${createHash("sha256").update(sid).digest("hex").slice(0, 12)}`;
