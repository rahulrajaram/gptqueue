import { describe, expect, it } from "vitest";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerRuntimeTools, type RuntimeTools } from "../src/registered-shell/runtime-tools.js";
import { SHELL_TOOL_NAMES } from "../src/registered-shell/tool-names.js";

describe("registered-shell tool catalog", () => {
  it("registers exactly the tools the shared catalog names, in order", () => {
    const names: string[] = [];
    const server = { tool: (name: string) => { names.push(name); } } as unknown as McpServer;
    registerRuntimeTools(server, {} as RedisClient, {} as RuntimeTools);
    expect(names).toEqual([...SHELL_TOOL_NAMES]);
  });
});
