import { describe, expect, it, vi } from "vitest";
import {
  OpenCodeSessionOwner,
  type OpenCodeSessionBackend,
  type OpenCodeSessionIdentity,
} from "../src/registered-shell/opencode-sessions.js";

const identity = (sessionID: string): OpenCodeSessionIdentity => ({
  sessionID,
  directory: `/tmp/${sessionID}`,
});

const backend = (label: string): OpenCodeSessionBackend => ({
  callTool: vi.fn(async () => ({ label })),
  close: vi.fn(async () => undefined),
});

describe("OpenCodeSessionOwner", () => {
  it("single-flights creation and reuses a backend for one exact session", async () => {
    const created: OpenCodeSessionBackend[] = [];
    const factory = vi.fn(async () => {
      const value = backend("parent");
      created.push(value);
      await Promise.resolve();
      return value;
    });
    const owner = new OpenCodeSessionOwner(factory);

    const [first, second] = await Promise.all([
      owner.getOrCreate(identity("parent")),
      owner.getOrCreate(identity("parent")),
    ]);

    expect(first).toBe(second);
    expect(factory).toHaveBeenCalledTimes(1);
    expect(created).toHaveLength(1);
    expect(owner.sessionIDs()).toEqual(["parent"]);
  });

  it("keeps parent and child ownership independent", async () => {
    const created = new Map<string, OpenCodeSessionBackend>();
    const owner = new OpenCodeSessionOwner(async ({ sessionID }) => {
      const value = backend(sessionID);
      created.set(sessionID, value);
      return value;
    });

    const parent = await owner.getOrCreate(identity("parent"));
    const child = await owner.getOrCreate(identity("child"));
    await parent.callTool("register_agent", { name: "parent-name" });
    await child.callTool("register_agent", { name: "child-name" });

    expect(parent.sessionID).not.toBe(child.sessionID);
    expect(created.get("parent")?.callTool).toHaveBeenCalledWith(
      "register_agent",
      { name: "parent-name" }
    );
    expect(created.get("child")?.callTool).toHaveBeenCalledWith(
      "register_agent",
      { name: "child-name" }
    );

    await owner.close("child");
    expect(created.get("child")?.close).toHaveBeenCalledTimes(1);
    expect(created.get("parent")?.close).not.toHaveBeenCalled();
    await expect(child.callTool("list_agents", {})).rejects.toThrow("closed");
    expect(owner.sessionIDs()).toEqual(["parent"]);
  });

  it("rejects a directory change for an already-owned session", async () => {
    const owner = new OpenCodeSessionOwner(async () => backend("one"));
    await owner.getOrCreate(identity("same-session"));

    expect(() =>
      owner.getOrCreate({ sessionID: "same-session", directory: "/tmp/other" })
    ).toThrow("already owned");
  });

  it("does not close a same-ID session from a different directory", async () => {
    const value = backend("same-id");
    const owner = new OpenCodeSessionOwner(async () => value);
    await owner.getOrCreate(identity("same-id"));

    await expect(owner.close("same-id", "/tmp/other")).resolves.toBe(false);
    expect(value.close).not.toHaveBeenCalled();
    expect(owner.sessionIDs()).toEqual(["same-id"]);

    await expect(owner.close("same-id", identity("same-id").directory)).resolves.toBe(true);
    expect(value.close).toHaveBeenCalledTimes(1);
  });

  it("removes failed creation so the same session can retry", async () => {
    const firstFailure = new Error("backend unavailable");
    const replacement = backend("replacement");
    const factory = vi
      .fn<({ sessionID }: OpenCodeSessionIdentity) => Promise<OpenCodeSessionBackend>>()
      .mockRejectedValueOnce(firstFailure)
      .mockResolvedValueOnce(replacement);
    const owner = new OpenCodeSessionOwner(factory);

    await expect(owner.getOrCreate(identity("retry"))).rejects.toBe(firstFailure);
    expect(owner.size).toBe(0);
    const handle = await owner.getOrCreate(identity("retry"));
    expect(handle).toBeDefined();
    expect(factory).toHaveBeenCalledTimes(2);
  });

  it("closes a backend that resolves after explicit retirement", async () => {
    let resolveBackend!: (value: OpenCodeSessionBackend) => void;
    const pending = new Promise<OpenCodeSessionBackend>((resolve) => {
      resolveBackend = resolve;
    });
    const lateBackend = backend("late");
    const owner = new OpenCodeSessionOwner(async () => pending);
    const opening = owner.getOrCreate(identity("closing"));
    const retiring = owner.close("closing");

    resolveBackend(lateBackend);
    await expect(opening).rejects.toThrow("closed during backend creation");
    await expect(retiring).resolves.toBe(true);
    expect(lateBackend.close).toHaveBeenCalledTimes(1);
    expect(owner.size).toBe(0);
  });

  it("disposes all sessions explicitly and has no idle retirement", async () => {
    const parent = backend("parent");
    const child = backend("child");
    const owner = new OpenCodeSessionOwner(
      async ({ sessionID }) => (sessionID === "parent" ? parent : child)
    );
    await owner.getOrCreate(identity("parent"));
    await owner.getOrCreate(identity("child"));

    expect(owner.size).toBe(2);
    await owner.dispose();

    expect(parent.close).toHaveBeenCalledTimes(1);
    expect(child.close).toHaveBeenCalledTimes(1);
    expect(owner.size).toBe(0);
  });

  it("marks disposal terminal before waiting for in-flight creation", async () => {
    let resolveBackend!: (value: OpenCodeSessionBackend) => void;
    const openingBackend = new Promise<OpenCodeSessionBackend>((resolve) => {
      resolveBackend = resolve;
    });
    const late = backend("late-dispose");
    const owner = new OpenCodeSessionOwner(async () => openingBackend);
    const opening = owner.getOrCreate(identity("pending"));
    const disposing = owner.dispose();

    expect(() => owner.getOrCreate(identity("new-after-dispose"))).toThrow("disposed");
    resolveBackend(late);
    await expect(opening).rejects.toThrow("closed during backend creation");
    await disposing;
    expect(late.close).toHaveBeenCalledTimes(1);
  });

  it("rejects calls as soon as handle close starts", async () => {
    let resolveClose!: () => void;
    const closeStarted = new Promise<void>((resolve) => { resolveClose = resolve; });
    const value: OpenCodeSessionBackend = {
      callTool: vi.fn(async () => ({ label: "closing" })),
      close: vi.fn(() => closeStarted),
    };
    const owner = new OpenCodeSessionOwner(async () => value);
    const handle = await owner.getOrCreate(identity("closing"));

    const closing = handle.close();
    await expect(handle.callTool("list_agents", {})).rejects.toThrow("closing or closed");
    resolveClose();
    await closing;
    expect(owner.size).toBe(0);
  });

  it("forwards tool cancellation to the identity-bound backend", async () => {
    const value = backend("abortable");
    const owner = new OpenCodeSessionOwner(async () => value);
    const handle = await owner.getOrCreate(identity("abortable"));
    const signal = new AbortController().signal;
    await handle.callTool("get_runtime_status", {}, signal);
    expect(value.callTool).toHaveBeenCalledWith("get_runtime_status", {}, signal);
    await owner.dispose();
  });

  it("surfaces a late backend close failure", async () => {
    let resolveBackend!: (value: OpenCodeSessionBackend) => void;
    const openingBackend = new Promise<OpenCodeSessionBackend>((resolve) => {
      resolveBackend = resolve;
    });
    const closeFailure = new Error("late close failed");
    const late: OpenCodeSessionBackend = {
      callTool: vi.fn(async () => ({ label: "leaky" })),
      close: vi.fn(async () => { throw closeFailure; }),
    };
    const owner = new OpenCodeSessionOwner(async () => openingBackend);
    const opening = owner.getOrCreate(identity("leaky"));
    const retiring = owner.close("leaky");

    resolveBackend(late);
    await expect(opening).rejects.toThrow("backend cleanup failed");
    await expect(retiring).rejects.toThrow("backend cleanup failed");
    expect(owner.size).toBe(0);
  });

  it("requires an absolute directory and bounded session ID", () => {
    const owner = new OpenCodeSessionOwner(async () => backend("invalid"));
    expect(() => owner.getOrCreate({ sessionID: "valid", directory: "relative" })).toThrow(
      "requires"
    );
    expect(() => owner.getOrCreate({ sessionID: "x".repeat(201), directory: "/tmp/valid" })).toThrow(
      "requires"
    );
  });
});
