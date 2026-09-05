import { describe, expect, it, vi } from "vitest";
import { renderPiExtension } from "../src/experimental-wrapper/config.js";
import { WRAPPER_VISIBLE_TOOLS } from "../src/experimental-wrapper/bridge.js";
import { requirePiTools, restrictPiTools } from "../src/experimental-wrapper/pi-tools.js";

const fakePi = () => {
  const tools = new Map<string, { name: string }>();
  let active: string[] = [];
  const hooks = new Map<string, () => Promise<void>>();
  return {
    registerTool: (tool: { name: string }) => {
      tools.set(tool.name, tool);
      active = [...active.filter((name) => name !== tool.name), tool.name];
    },
    setActiveTools: (names: string[]) => { active = [...names]; },
    getActiveTools: () => [...active],
    getAllTools: () => [...tools.values()],
    on: (name: string, handler: () => Promise<void>) => { hooks.set(name, handler); },
    hooks,
  };
};

describe("Pi messaging-only tool boundary", () => {
  it("blocks proxy/script registration and later attempts to activate them", async () => {
    const pi = fakePi();
    const guarded = restrictPiTools(pi, WRAPPER_VISIBLE_TOOLS);
    for (const name of ["mcp", "mcpScript", "bash", ...WRAPPER_VISIBLE_TOOLS]) {
      guarded.registerTool({ name });
    }
    guarded.setActiveTools(["mcpScript", ...WRAPPER_VISIBLE_TOOLS, "mcp"]);
    expect(pi.getActiveTools()).toEqual([...WRAPPER_VISIBLE_TOOLS]);
    expect(pi.getAllTools().map((tool) => tool.name)).toEqual([...WRAPPER_VISIBLE_TOOLS]);
    expect(await requirePiTools(pi, WRAPPER_VISIBLE_TOOLS)).toEqual([...WRAPPER_VISIBLE_TOOLS]);
  });

  it("waits for asynchronous discovery and fails when a required tool is missing", async () => {
    const pi = fakePi();
    await expect(requirePiTools(pi, WRAPPER_VISIBLE_TOOLS, 0)).rejects.toThrow(/unavailable/);
    const waiting = requirePiTools(pi, WRAPPER_VISIBLE_TOOLS, 1_000);
    for (const name of WRAPPER_VISIBLE_TOOLS) pi.registerTool({ name });
    expect(await waiting).toEqual([...WRAPPER_VISIBLE_TOOLS]);
  });

  it("rejects a runtime that keeps an extra tool active", async () => {
    const pi = fakePi();
    for (const name of WRAPPER_VISIBLE_TOOLS) pi.registerTool({ name });
    pi.getActiveTools = () => [...WRAPPER_VISIBLE_TOOLS, "mcpScript"];
    await expect(requirePiTools(pi, WRAPPER_VISIBLE_TOOLS)).rejects.toThrow(/Unexpected/);
  });

  it("wires the rendered extension to the guard and checks tools before inference", async () => {
    const pi = fakePi();
    const configurations: Array<{ config: { settings: { scriptMode: boolean } } }> = [];
    const createMcpAdapter = (config: typeof configurations[number]) => {
      configurations.push(config);
      return (api: ReturnType<typeof fakePi>) => {
        for (const name of ["mcp", "mcpScript", ...WRAPPER_VISIBLE_TOOLS]) api.registerTool({ name });
      };
    };
    const source = renderPiExtension("/inspected/adapter.ts")
      .replace(/^import .*;$/gm, "")
      .replace("export default function", "function");
    const exit = vi.fn(() => { throw new Error("child terminated"); });
    const localConsole = { error: vi.fn() };
    const install = new Function("createMcpAdapter", "restrictPiTools", "requirePiTools", "process", "console",
      source + "\nreturn messagingOnlyPi;")(
      createMcpAdapter, restrictPiTools, requirePiTools,
      { env: { GPTQ_PI_ADAPTER_STATE_DIR: "/local-state", GPTQ_BRIDGE_URL: "http://127.0.0.1:1/mcp" }, exit }, localConsole
    );
    install(pi);
    expect(configurations[0]!.config.settings.scriptMode).toBe(false);
    expect(pi.hooks.has("before_agent_start")).toBe(true);
    await pi.hooks.get("before_agent_start")!();
    expect(pi.getActiveTools()).toEqual([...WRAPPER_VISIBLE_TOOLS]);
    expect(exit).not.toHaveBeenCalled();
    pi.getActiveTools = () => [...WRAPPER_VISIBLE_TOOLS, "mcpScript"];
    await expect(pi.hooks.get("before_agent_start")!()).rejects.toThrow("child terminated");
    expect(exit).toHaveBeenCalledWith(1);
  });
});
