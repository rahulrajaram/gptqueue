import { setTimeout as delay } from "node:timers/promises";

/** The small part of Pi's local API needed to enforce the child tool surface. */
export interface PiToolApi {
  registerTool(tool: { readonly name: string }): void;
  setActiveTools(names: string[]): void;
  getActiveTools(): string[];
  getAllTools(): { readonly name: string }[];
}

const sameTools = (actual: readonly string[], allowed: readonly string[]): boolean =>
  actual.length === allowed.length &&
  allowed.every((name) => actual.includes(name));

/** The adapter may register a fallback proxy even when disableProxyTool is set. */
export const restrictPiTools = <T extends PiToolApi>(
  pi: T,
  allowed: readonly string[]
): T => ({
  ...pi,
  registerTool: (tool) => {
    if (allowed.includes(tool.name)) pi.registerTool(tool);
  },
  setActiveTools: (names) =>
    pi.setActiveTools(names.filter((name) => allowed.includes(name))),
});

/** Wait for asynchronous discovery, then inspect the actual active catalog. */
export const requirePiTools = async (
  pi: PiToolApi,
  allowed: readonly string[],
  timeoutMs = 15_000
): Promise<readonly string[]> => {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    const registered = pi.getAllTools().map((tool) => tool.name);
    if (allowed.every((name) => registered.includes(name))) {
      pi.setActiveTools([...allowed]);
      const active = pi.getActiveTools();
      if (!sameTools(active, allowed)) {
        throw new Error(`Unexpected Pi active tools: ${JSON.stringify(active)}`);
      }
      return Object.freeze([...active]);
    }
    if (Date.now() >= deadline) {
      throw new Error(`Pi messaging tools unavailable: ${JSON.stringify(registered)}`);
    }
    await delay(25);
  }
};
