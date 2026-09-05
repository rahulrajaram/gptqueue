/** Pure acceptance criteria, shared by the live driver and its negative tests. */
export const verificationChecks = ({ failure, proof, exits, remaining, before, after, newFiles, piRuntime, retryId, replyRetryId }) => Object.freeze({
  exchange: failure === null && proof !== null,
  exits: exits.length === 2 && exits.every(result => result.code === 0),
  retry_record: proof !== null && retryId === proof.sent_id && replyRetryId === proof.reply_id,
  redis_empty: remaining.length === 0,
  global_contents: Object.keys(before).every(key => before[key]?.sha256 === after[key]?.sha256),
  pi_cache_metadata: JSON.stringify(before.pi_cache) === JSON.stringify(after.pi_cache),
  local_pi_files: newFiles.length > 0 && newFiles.every(path => path.startsWith(piRuntime)),
});
