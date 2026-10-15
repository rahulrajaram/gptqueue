/**
 * The seat contract suite (AT10). Every SeatStore adapter must pass it
 * unchanged; it carries AT2 (exclusivity), AT3 (dead-holder release),
 * AT4 (fencing) and AT5 (tenure history) at the service level.
 */
import { describe, expect, it } from "vitest";
import {
  createSeatService,
  DEFAULT_LEASE_SECONDS,
  type SeatService,
  type SeatStore,
  type SeatTenure,
  type TenureHistoryEntry,
} from "../../src/agent-tracking/index.js";
import { fakeClock, fakeProbe, holder, otherHolder } from "./fixtures.js";

const SEAT = "acme.supervisor";
const LEASE_MS = DEFAULT_LEASE_SECONDS * 1000;

type Rig = Readonly<{
  store: SeatStore;
  service: SeatService;
  clock: ReturnType<typeof fakeClock>;
  probe: ReturnType<typeof fakeProbe>;
}>;

const expectOk = <T>(result: { ok: true; value: T } | { ok: false; error: { code: string; message: string } }): T => {
  if (!result.ok) throw new Error(`expected ok, got ${result.error.code}: ${result.error.message}`);
  return result.value;
};

const expectCode = (
  result: { ok: true } | { ok: false; error: { code: string } },
  code: string
): void => {
  expect(result.ok).toBe(false);
  if (!result.ok) expect(result.error.code).toBe(code);
};

export const seatContract = (name: string, makeStore: () => Promise<SeatStore> | SeatStore): void => {
  const rig = async (): Promise<Rig> => {
    const store = await makeStore();
    const clock = fakeClock();
    const probe = fakeProbe();
    probe.run(holder().pid, holder().pid_start);
    probe.run(otherHolder().pid, otherHolder().pid_start);
    return { store, clock, probe, service: createSeatService({ store, clock, probe }) };
  };

  const tenureOf = async (r: Rig): Promise<SeatTenure> => expectOk(await r.service.status(SEAT));
  const historyOf = async (r: Rig): Promise<readonly TenureHistoryEntry[]> => expectOk(await r.service.history(SEAT));

  describe(`seat contract: ${name}`, () => {
    describe("store semantics", () => {
      it("reads an unknown seat as absent and commits only at the expected revision", async () => {
        const r = await rig();
        expect(await r.store.read(SEAT)).toEqual({ ok: true, stored: null });
        const tenure: SeatTenure = { seat_id: SEAT, state: "vacant", epoch: 0 };
        expect(await r.store.commit(SEAT, 0, tenure, [])).toEqual({ ok: true, committed: true });
        expect(await r.store.commit(SEAT, 0, tenure, [])).toEqual({ ok: true, committed: false });
        const read = await r.store.read(SEAT);
        expect(read.ok && read.stored?.rev).toBe(1);
      });

      it("appends history only with a successful commit", async () => {
        const r = await rig();
        const entry: TenureHistoryEntry = {
          kind: "bind", seat_id: SEAT, epoch: 1, reason: "vacant",
          runtime: "claude", session_id: "s", at: r.clock.iso(),
        };
        const held: SeatTenure = { seat_id: SEAT, state: "vacant", epoch: 1 };
        expect(await r.store.commit(SEAT, 0, held, [entry])).toEqual({ ok: true, committed: true });
        expect(await r.store.commit(SEAT, 0, held, [entry])).toEqual({ ok: true, committed: false });
        expect(await r.store.history(SEAT)).toEqual({ ok: true, entries: [entry] });
      });

      it("keeps seats independent", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        expect(expectOk(await r.service.status("other.seat"))).toEqual({ seat_id: "other.seat", state: "vacant", epoch: 0 });
        expect(expectOk(await r.service.history("other.seat"))).toEqual([]);
      });
    });

    describe("AT2 exclusivity", () => {
      it("binds a vacant seat at epoch 1 for the requesting process", async () => {
        const r = await rig();
        const step = expectOk(await r.service.bind(SEAT, holder()));
        expect(step.tenure).toMatchObject({ seat_id: SEAT, state: "held", epoch: 1, holder: holder(), bound_at: r.clock.iso() });
        expect(step.tenure.lease_expires_at).toBe(new Date(r.clock.now().getTime() + LEASE_MS).toISOString());
        expect(await tenureOf(r)).toEqual(step.tenure);
      });

      it("refuses a second live process with seat_held and leaves the tenure untouched", async () => {
        const r = await rig();
        const first = expectOk(await r.service.bind(SEAT, holder())).tenure;
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
        expect(await tenureOf(r)).toEqual(first);
        expect(await historyOf(r)).toHaveLength(1);
      });

      it("treats a rebind by the same process as idempotent: same epoch, lease renewed, no new tenure", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.clock.advance(60_000);
        const again = expectOk(await r.service.bind(SEAT, holder()));
        expect(again.tenure.epoch).toBe(1);
        expect(again.tenure.lease_expires_at).toBe(new Date(r.clock.now().getTime() + LEASE_MS).toISOString());
        expect(again.entries).toEqual([]);
        expect(await historyOf(r)).toHaveLength(1);
      });

      it("keeps the tenure when the same process starts a new native session, recording a rebind", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        const cleared = holder({ session_id: "33333333-3333-4333-8333-333333333333" });
        const step = expectOk(await r.service.bind(SEAT, cleared));
        expect(step.tenure.epoch).toBe(1);
        expect(step.tenure.holder?.session_id).toBe(cleared.session_id);
        expect(step.entries).toEqual([{
          kind: "rebind", seat_id: SEAT, epoch: 1, runtime: "claude",
          session_id: cleared.session_id, previous_session_id: holder().session_id, at: r.clock.iso(),
        }]);
      });

      it("refuses when the same session id arrives from a different live process (double resume)", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.run(4303, "900003");
        expectCode(await r.service.bind(SEAT, holder({ pid: 4303, pid_start: "900003" })), "seat_held");
      });

      it("resolves concurrent binds on a vacant seat to exactly one holder", async () => {
        const r = await rig();
        const results = await Promise.all([r.service.bind(SEAT, holder()), r.service.bind(SEAT, otherHolder())]);
        expect(results.filter((x) => x.ok)).toHaveLength(1);
        const refused = results.filter((x) => !x.ok);
        expect(refused).toHaveLength(1);
        expect(refused[0]?.ok === false && refused[0].error.code).toBe("seat_held");
        expect((await tenureOf(r)).epoch).toBe(1);
        expect(await historyOf(r)).toHaveLength(1);
      });

      it("refuses a holder on another host while its lease is unexpired", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder({ host: "host-b" })));
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
      });

      it("resolves concurrent takeovers of an exited holder to exactly one successor", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.kill(holder().pid);
        const binders = [otherHolder(), otherHolder({ pid: 4303, pid_start: "900003" }), otherHolder({ pid: 4404, pid_start: "900004" })];
        for (const b of binders) r.probe.run(b.pid, b.pid_start);
        const results = await Promise.all(binders.map((b) => r.service.bind(SEAT, b)));
        expect(results.filter((x) => x.ok)).toHaveLength(1);
        expect(results.filter((x) => !x.ok).map((x) => !x.ok && x.error.code)).toEqual(["seat_held", "seat_held"]);
        expect((await tenureOf(r)).epoch).toBe(2);
        expect((await historyOf(r)).map((e) => [e.kind, e.epoch])).toEqual([["bind", 1], ["vacate", 1], ["bind", 2]]);
      });

      it("reports contention, and writes nothing, when every compare-and-set loses", async () => {
        const r = await rig();
        const losing: SeatStore = { ...r.store, commit: async () => ({ ok: true, committed: false }) };
        const service = createSeatService({ store: losing, clock: r.clock, probe: r.probe, max_attempts: 3 });
        expectCode(await service.bind(SEAT, holder()), "contention");
        expect(await r.store.read(SEAT)).toEqual({ ok: true, stored: null });
      });
    });

    describe("AT3 dead-holder release", () => {
      it("takes over immediately when the holder process has provably exited on this host", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.kill(holder().pid);
        const step = expectOk(await r.service.bind(SEAT, otherHolder()));
        expect(step.tenure).toMatchObject({ state: "held", epoch: 2, holder: otherHolder() });
        expect(step.entries.map((e) => [e.kind, e.epoch, "reason" in e ? e.reason : null])).toEqual([
          ["vacate", 1, "holder_exit_proven"],
          ["bind", 2, "takeover"],
        ]);
      });

      it("treats a recycled pid (running, different start time) as an exited holder", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.run(holder().pid, "999999");
        expect(expectOk(await r.service.bind(SEAT, otherHolder())).tenure.epoch).toBe(2);
      });

      it("never vacates a provably live holder, even long after its lease expired", async () => {
        const r = await rig();
        const first = expectOk(await r.service.bind(SEAT, holder())).tenure;
        r.clock.advance(LEASE_MS * 3);
        expect(expectOk(await r.service.reap(SEAT))).toEqual(first);
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
      });

      it("does not vacate an unobservable holder before its lease expires (silence is not exit)", async () => {
        const r = await rig();
        const first = expectOk(await r.service.bind(SEAT, holder({ host: "host-b" }))).tenure;
        r.clock.advance(LEASE_MS - 1);
        expect(expectOk(await r.service.reap(SEAT))).toEqual(first);
      });

      it("vacates an unobservable holder once its lease has expired", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder({ host: "host-b" })));
        r.clock.advance(LEASE_MS + 1);
        const reaped = expectOk(await r.service.reap(SEAT));
        expect(reaped).toEqual({ seat_id: SEAT, state: "vacant", epoch: 1 });
        const last = (await historyOf(r)).at(-1);
        expect(last).toMatchObject({ kind: "vacate", epoch: 1, reason: "lease_expired" });
      });

      it("treats a holder in another PID namespace, or with unreadable pidns, as unobservable", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder({ pidns: "pid:[999]" })));
        r.probe.kill(holder().pid);
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");

        const r2 = await rig();
        expectOk(await r2.service.bind(SEAT, holder({ pidns: null })));
        r2.probe.kill(holder().pid);
        expectCode(await r2.service.bind(SEAT, otherHolder()), "seat_held");
      });

      it("treats a holder on another machine with the same hostname and pidns as unobservable", async () => {
        // Cloned hosts share hostname and the initial pidns inode; only machine-id and boot_id tell them apart.
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder({ machine_id: "0".repeat(32), boot_id: "other-boot" })));
        r.probe.run(holder().pid, "unrelated-start"); // an unrelated local process happens to reuse the pid
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
        r.clock.advance(LEASE_MS + 1);
        expect(expectOk(await r.service.bind(SEAT, otherHolder())).entries[0]).toMatchObject({ kind: "vacate", reason: "lease_expired" });
      });

      it("treats a holder from an earlier boot of this machine as exited, without waiting for its lease", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder({ boot_id: "earlier-boot" })));
        const step = expectOk(await r.service.bind(SEAT, otherHolder()));
        expect(step.tenure.epoch).toBe(2);
        expect(step.entries[0]).toMatchObject({ kind: "vacate", reason: "holder_exit_proven" });
      });

      it("treats a holder with an unreadable machine or boot identity as unobservable", async () => {
        for (const missing of [{ machine_id: null }, { boot_id: null }]) {
          const r = await rig();
          expectOk(await r.service.bind(SEAT, holder(missing)));
          r.probe.kill(holder().pid);
          expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
        }
      });

      it("treats an unreadable local process as unobservable, not as exited", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.unreadable(holder().pid);
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
      });

      it("reaps a provably exited holder without waiting: no clock advance needed", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.kill(holder().pid);
        expect(expectOk(await r.service.reap(SEAT))).toEqual({ seat_id: SEAT, state: "vacant", epoch: 1 });
        expect((await historyOf(r)).at(-1)).toMatchObject({ kind: "vacate", reason: "holder_exit_proven", at: r.clock.iso() });
      });
    });

    describe("AT4 fencing", () => {
      it("rejects the predecessor's renew, release and fence after a takeover", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.probe.kill(holder().pid);
        expectOk(await r.service.bind(SEAT, otherHolder()));
        expectCode(await r.service.renew(SEAT, holder(), 1), "stale_epoch");
        expectCode(await r.service.release(SEAT, holder(), 1), "stale_epoch");
        expectCode(await r.service.fence(SEAT, 1), "stale_epoch");
        expect(expectOk(await r.service.fence(SEAT, 2)).epoch).toBe(2);
      });

      it("rejects a renew or release from a non-holder presenting the current epoch", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        expectCode(await r.service.renew(SEAT, otherHolder(), 1), "not_holder");
        expectCode(await r.service.release(SEAT, otherHolder(), 1), "not_holder");
      });

      it("rejects every epoch once the seat is vacant", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        expectOk(await r.service.release(SEAT, holder(), 1));
        expectCode(await r.service.fence(SEAT, 1), "stale_epoch");
        expectCode(await r.service.renew(SEAT, holder(), 1), "stale_epoch");
        expectCode(await r.service.fence(SEAT, 0), "stale_epoch");
      });

      it("renews the lease for the current holder without changing the epoch", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        r.clock.advance(5_000);
        const renewed = expectOk(await r.service.renew(SEAT, holder(), 1));
        expect(renewed.epoch).toBe(1);
        expect(renewed.lease_expires_at).toBe(new Date(r.clock.now().getTime() + LEASE_MS).toISOString());
      });

      it("issues strictly increasing epochs across release, rebind and takeover", async () => {
        const r = await rig();
        const epochs: number[] = [];
        epochs.push(expectOk(await r.service.bind(SEAT, holder())).tenure.epoch);
        expectOk(await r.service.release(SEAT, holder(), 1));
        epochs.push(expectOk(await r.service.bind(SEAT, holder())).tenure.epoch);
        r.probe.kill(holder().pid);
        epochs.push(expectOk(await r.service.bind(SEAT, otherHolder())).tenure.epoch);
        expect(epochs).toEqual([1, 2, 3]);
      });
    });

    describe("AT5 tenure history", () => {
      it("records bind, rebind, vacate and takeover with session, time and reason, append-only", async () => {
        const r = await rig();
        const at: string[] = [];
        expectOk(await r.service.bind(SEAT, holder())); at.push(r.clock.iso());
        const prefix = await historyOf(r);

        r.clock.advance(1_000);
        const cleared = holder({ session_id: "33333333-3333-4333-8333-333333333333" });
        expectOk(await r.service.bind(SEAT, cleared)); at.push(r.clock.iso());
        r.clock.advance(1_000);
        expectOk(await r.service.release(SEAT, cleared, 1)); at.push(r.clock.iso());
        r.clock.advance(1_000);
        expectOk(await r.service.bind(SEAT, holder())); at.push(r.clock.iso());
        r.clock.advance(1_000);
        r.probe.kill(holder().pid);
        expectOk(await r.service.bind(SEAT, otherHolder())); at.push(r.clock.iso());

        const history = await historyOf(r);
        expect(history.slice(0, prefix.length)).toEqual(prefix);
        expect(history).toEqual([
          { kind: "bind", seat_id: SEAT, epoch: 1, reason: "vacant", runtime: "claude", session_id: holder().session_id, at: at[0] },
          { kind: "rebind", seat_id: SEAT, epoch: 1, runtime: "claude", session_id: cleared.session_id, previous_session_id: holder().session_id, at: at[1] },
          { kind: "vacate", seat_id: SEAT, epoch: 1, reason: "released", runtime: "claude", session_id: cleared.session_id, at: at[2] },
          { kind: "bind", seat_id: SEAT, epoch: 2, reason: "vacant", runtime: "claude", session_id: holder().session_id, at: at[3] },
          { kind: "vacate", seat_id: SEAT, epoch: 2, reason: "holder_exit_proven", runtime: "claude", session_id: holder().session_id, at: at[4] },
          { kind: "bind", seat_id: SEAT, epoch: 3, reason: "takeover", runtime: "claude", session_id: otherHolder().session_id, at: at[4] },
        ]);
      });

      it("appends nothing for refused operations or a no-op reap", async () => {
        const r = await rig();
        expectOk(await r.service.bind(SEAT, holder()));
        expectCode(await r.service.bind(SEAT, otherHolder()), "seat_held");
        expectCode(await r.service.renew(SEAT, otherHolder(), 1), "not_holder");
        expectCode(await r.service.fence(SEAT, 7), "stale_epoch");
        expectOk(await r.service.reap(SEAT));
        expect(await historyOf(r)).toHaveLength(1);
      });
    });
  });
};
