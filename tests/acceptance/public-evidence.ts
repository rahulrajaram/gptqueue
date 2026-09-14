/** Redact credentials while preserving the original evidence shape and public message IDs. */
export type EvidenceOptions = Readonly<{ redactSessionObjectIds?: boolean; parseEmbeddedJson?: boolean }>;
const secretKey = /authorization|(?:access|refresh)[_-]?token|api[_-]?key|password|secret|credential|session[_-]?id|thinkingSignature|^signature$/iu;
const secretText = /(Bearer\s+)\S+|((?:session[_-]?id|api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret)["']?\s*[:=]\s*["']?)[^\s,"'}]+/giu;
const privateContent = (value: unknown): boolean => Boolean(value && typeof value === 'object' &&
  ['thinking', 'reasoning'].includes(String((value as Record<string, unknown>).type)));

export function sanitizeEvidence(value: unknown, options: EvidenceOptions = {}, path = ''): unknown {
  if (typeof value === 'string') {
    if (/^[[{]/u.test(value.trim())) {
      try {
        const parsed = sanitizeEvidence(JSON.parse(value), options, path);
        return options.parseEmbeddedJson ? parsed : JSON.stringify(parsed);
      } catch { /* plain text */ }
    }
    return value.replace(secretText, (_match, bearer: string | undefined, assignment: string | undefined) =>
      `${bearer ?? assignment ?? ''}[redacted]`);
  }
  if (Array.isArray(value)) return value.filter(item => !privateContent(item)).map(item => sanitizeEvidence(item, options, path));
  if (!value || typeof value !== 'object') return value;
  if (privateContent(value)) return { type: '[private content removed]' };
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key,
    secretKey.test(key) || (options.redactSessionObjectIds && key === 'id' && /session|info/iu.test(path))
      ? '[redacted]' : sanitizeEvidence(item, options, `${path}.${key}`)]));
}
