import { it, expect } from 'vitest';
import { reportsTotal, reportsCount } from './answer.js';

it('accepts a declared correct total with a coherent calculation', () => {
  expect(reportsTotal('Combined total: **2709 + 6929 = 9638**', '9638')).toBe(true);
  expect(reportsTotal('- My count: 5744\n- **Combined total: 11858**', '11858')).toBe(true);
});
it('requires an explicit count rather than an incidental expected number', () => {
  expect(reportsCount('My verified count for this exercise is 6579. Your count was 5722.', '6579')).toBe(true);
  expect(reportsCount('The verified count is 6578. I saw 6579 before.', '6579')).toBe(false);
  expect(reportsCount('The verified count is 6579. My verified count is 6578.', '6579')).toBe(false);
  expect(reportsCount('65790', '6579')).toBe(false);
});
it('rejects a correct number mentioned incidentally beside a wrong answer', () => {
  expect(reportsTotal('The expected number might be 9638.\nCombined total: 9639', '9638')).toBe(false);
  expect(reportsTotal('Combined total: 9638\nSum: 9639', '9638')).toBe(false);
  expect(reportsTotal('Combined total: 2709 + 6928 = 9638', '9638')).toBe(false);
});
