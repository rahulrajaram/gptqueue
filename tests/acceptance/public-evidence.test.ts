import { it, expect } from 'vitest';
import { sanitizeEvidence } from './public-evidence.js';

it('redacts nested JSON credentials without changing text fields into objects', () => {
  const value = { content: [{ text: JSON.stringify({ session_id: 'private', accessToken: 'private', id: 'message-1' }) }] };
  expect(sanitizeEvidence(value)).toEqual({ content: [{ text: JSON.stringify({ session_id: '[redacted]', accessToken: '[redacted]', id: 'message-1' }) }] });
  expect(sanitizeEvidence(value, { parseEmbeddedJson: true })).toEqual({ content: [{ text: { session_id: '[redacted]', accessToken: '[redacted]', id: 'message-1' } }] });
  expect(sanitizeEvidence('Bearer private session_id="private" api-key=private')).toBe('Bearer [redacted] session_id="[redacted]" api-key=[redacted]');
});
it('retains exact public correlation and claim evidence while removing reasoning', () => {
  const proof = { id: 'reply', in_reply_to: 'request', claim_id: 'claim', runtime_id: 'runtime', content: [{ type: 'thinking', thinking: 'private' }, { type: 'text', text: '42' }] };
  expect(sanitizeEvidence(proof)).toEqual({ ...proof, content: [{ type: 'text', text: '42' }] });
  const payload = 'x'.repeat(5000);
  expect(sanitizeEvidence({ content: payload })).toEqual({ content: payload });
});
it('redacts native session object IDs only when the route declares that shape', () => {
  const proof = { session: { id: 'private' }, message: { id: 'public' } };
  expect(sanitizeEvidence(proof, { redactSessionObjectIds: true })).toEqual({ session: { id: '[redacted]' }, message: { id: 'public' } });
  expect(sanitizeEvidence(proof)).toEqual(proof);
});
