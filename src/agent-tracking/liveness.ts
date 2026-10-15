/**
 * Judge a holder's liveness from the caller's machine. Evidence is conclusive
 * only on the same machine: hostnames and the initial PID namespace inode are
 * shared by cloned hosts, so they are guards, never proof. machine-id must be
 * unique per host (a systemd requirement); boot_id is random per boot.
 */
import type { ProcessProbe } from "./ports.js";
import type { HolderIdentity, Liveness } from "./types.js";

export const judgeLiveness = (holder: HolderIdentity, probe: ProcessProbe): Liveness => {
  const local = probe.local();
  if (holder.machine_id === null || local.machine_id === null || holder.machine_id !== local.machine_id || holder.host !== local.host) {
    return "unknown";
  }
  if (holder.boot_id === null || local.boot_id === null) return "unknown";
  // Same machine, different boot: the holder's boot has ended, and every process of it with it.
  if (holder.boot_id !== local.boot_id) return "exited";
  // Same boot: a pid is only comparable inside the same PID namespace (containers).
  if (holder.pidns === null || local.pidns === null || holder.pidns !== local.pidns) return "unknown";
  const seen = probe.observe(holder.pid);
  switch (seen.state) {
    case "absent":
      return "exited";
    case "running":
      return seen.start === holder.pid_start ? "alive" : "exited"; // a recycled pid is a different process
    case "unreadable":
      return "unknown";
    default: {
      const unreachable: never = seen;
      throw new Error(`Unhandled observation: ${JSON.stringify(unreachable)}`);
    }
  }
};
