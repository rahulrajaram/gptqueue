import { describe, expect, it } from "vitest";
import { agentAttributionEnv } from "../src/pty-wrapper/attribution.js";

describe("agentAttributionEnv", () => {
  it("provides generic defaults when no overrides are inherited", () => {
    expect(
      agentAttributionEnv("agent-7", {}, "/home/user/my-project")
    ).toEqual({
      AGENT_ATTRIBUTION_CALLER: "gptqueue-pty",
      AGENT_ATTRIBUTION_PROJECT: "my-project",
      AGENT_ATTRIBUTION_SESSION: "agent-7",
    });
  });

  it("preserves inherited nonempty overrides", () => {
    expect(
      agentAttributionEnv(
        "agent-7",
        {
          AGENT_ATTRIBUTION_CALLER: "custom-caller",
          AGENT_ATTRIBUTION_PROJECT: "custom-project",
          AGENT_ATTRIBUTION_SESSION: "custom-session",
        },
        "/home/user/my-project"
      )
    ).toEqual({
      AGENT_ATTRIBUTION_CALLER: "custom-caller",
      AGENT_ATTRIBUTION_PROJECT: "custom-project",
      AGENT_ATTRIBUTION_SESSION: "custom-session",
    });
  });

  it("falls back to defaults for empty-string overrides", () => {
    const env = agentAttributionEnv(
      "agent-7",
      { AGENT_ATTRIBUTION_CALLER: "" },
      "/x/queue-runner"
    );
    expect(env.AGENT_ATTRIBUTION_CALLER).toBe("gptqueue-pty");
    expect(env.AGENT_ATTRIBUTION_PROJECT).toBe("queue-runner");
    expect(env.AGENT_ATTRIBUTION_SESSION).toBe("agent-7");
  });
});
