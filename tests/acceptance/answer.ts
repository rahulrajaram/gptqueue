/** Recognize a declared total, not an incidental occurrence in prose or a tool log. */
export const reportsTotal = (text: string, expected: string): boolean => {
  const answers = text.replaceAll('**', '').split('\n').flatMap(line => {
    const match = line.trim().replace(/^[-*+]\s+/u, '').match(/^(?:(?:combined total|sum):\s*)?(?:(\d+)\s*\+\s*(\d+)\s*=\s*)?(\d+)$/iu);
    if (!match) return [];
    const [, left, right, total] = match;
    return [{ total, coherent: left === undefined || Number(left) + Number(right) === Number(total) }];
  });
  return answers.length > 0 && answers.every(answer => answer.coherent && answer.total === expected);
};

/** Accept only an explicit count declaration or a decimal-only response. */
export const reportsCount = (text: string, expected: string): boolean => {
  const normalized = text.replaceAll('**', '').trim();
  if (/^\d+$/u.test(normalized)) return normalized === expected;
  const declarations = [...normalized.matchAll(/\b(?:my |the )?verified count(?: for this exercise)? is\s+(\d+)\b/giu)];
  return declarations.length > 0 && declarations.every(match => match[1] === expected);
};
