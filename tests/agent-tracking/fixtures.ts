/** Deterministic fixtures for the agent-tracking acceptance tests. */
import type {
  Clock,
  HolderIdentity,
  LocalIdentity,
  ProcessObservation,
  ProcessProbe,
  SeatDeclaration,
} from "../../src/agent-tracking/index.js";

export const T0 = Date.parse("2030-01-01T00:00:00.000Z");
export const HOST = "host-a";
export const MACHINE = "f913fd234fbd437cbe0c8b1b8e31412c";
export const BOOT = "d6ebbdbf-ed14-4c24-af00-20f59745b9b7";
/** The initial PID namespace inode: the same constant on every Linux host. */
export const PIDNS = "pid:[4026531836]";

/** A clock the test advances explicitly. */
export const fakeClock = (start = T0): Clock & { advance(ms: number): void; iso(): string } => {
  let now = start;
  return {
    now: () => new Date(now),
    advance: (ms: number) => { now += ms; },
    iso: () => new Date(now).toISOString(),
  };
};

/** A process table the test edits: pid -> start, or absent. */
export const fakeProbe = (
  local: LocalIdentity = { host: HOST, machine_id: MACHINE, boot_id: BOOT, pidns: PIDNS }
): ProcessProbe & {
  run(pid: number, start: string): void;
  kill(pid: number): void;
  unreadable(pid: number): void;
} => {
  const table = new Map<number, ProcessObservation>();
  return {
    local: () => local,
    observe: (pid: number) => table.get(pid) ?? { state: "absent" },
    run: (pid: number, start: string) => { table.set(pid, { state: "running", start }); },
    kill: (pid: number) => { table.delete(pid); },
    unreadable: (pid: number) => { table.set(pid, { state: "unreadable" }); },
  };
};

export const holder = (overrides: Partial<HolderIdentity> = {}): HolderIdentity => ({
  runtime: "claude",
  session_id: "11111111-1111-4111-8111-111111111111",
  host: HOST,
  machine_id: MACHINE,
  boot_id: BOOT,
  pidns: PIDNS,
  pid: 4101,
  pid_start: "900001",
  ...overrides,
});

/** A second, distinct live process on the same host. */
export const otherHolder = (overrides: Partial<HolderIdentity> = {}): HolderIdentity =>
  holder({ session_id: "22222222-2222-4222-8222-222222222222", pid: 4202, pid_start: "900002", ...overrides });

export const seat = (overrides: Partial<SeatDeclaration> = {}): SeatDeclaration => ({
  seat_id: "acme.supervisor",
  name: "Acme supervisor",
  description:
    "Coordinates the Acme leads, answers their decision packets, and owns the merge gate to master.",
  capabilities: ["merge-gate", "decision-packets"],
  policy: { runtimes: ["claude"], cwd_roots: ["/workspace/project-a"] },
  ...overrides,
});

export const seatFileJson = (seats: readonly SeatDeclaration[] = [seat()]): string =>
  JSON.stringify({ schema_version: 1, seats }, null, 2);

/** Matches text that would ask the model to make a binding or registration tool call. */
export const BINDING_TOOL_INSTRUCTION =
  /\b(register_agent|actor_register|bind_runtime|custody_claim|set_agent_profile)\b|\bcall\s+\w+\s+to\s+(bind|hold|claim|register)/i;
