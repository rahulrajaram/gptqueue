/**
 * Operator alerts (loop adapter S3), behind an operator-owned setting.
 *
 * The watchdog's `escalate` effect hands each alert to an OperatorAlertSink.
 * In this slice the only admitted mode is `dry_run`: the would-be email is
 * written as an RFC 5322 file to the outbox, with the recipient redacted as
 * `<operator>`. No transport is enabled: every sender is a stub that refuses
 * with `transport_not_enabled`, and only tests inject a fake one.
 *
 * Secrets are referenced, never read: `transport.password_file` is a path and
 * `transport.api_key_env` is an environment variable NAME. Admission errors
 * name the offending field, never its value. The operator address is used
 * only as the envelope recipient handed to a transport; it is never written
 * to a file, a log line or command output.
 */
import { createHash, randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { mkdir, open, readFile, rename, writeFile, type FileHandle } from "node:fs/promises";
import { join } from "node:path";

const assertNever = (value: never): never => {
  throw new Error(`Unhandled operator-alert value: ${JSON.stringify(value)}`);
};

// ---------------------------------------------------------------- config

export type TransportKind = "direct_mx" | "smtp_auth" | "http_api";
export type QuietPolicy = "send" | "hold_and_summarize";
export type AlertMode = "dry_run" | "live";

export interface QuietHours {
  readonly tz: string;
  /** "HH:MM", local to `tz`. */
  readonly start: string;
  readonly end: string;
  readonly policy: QuietPolicy;
}

export interface OperatorAlertsConfig {
  readonly schema_version: 1;
  readonly enabled: boolean;
  /** Admission only ever yields `dry_run` in this slice; `live` exists for the fake-transport unit test. */
  readonly mode: AlertMode;
  readonly transport: Readonly<{ kind: TransportKind; password_file?: string; api_key_env?: string }> & Readonly<Record<string, unknown>>;
  readonly to: string;
  readonly from: string;
  readonly quiet_hours: QuietHours;
  readonly rate_limit: Readonly<{ max_per_hour: number }>;
}

export type AlertsConfigError =
  | "alerts_config_invalid"
  | "alerts_mode_live_refused"
  | "alerts_transport_unknown"
  | "alerts_inline_secret"
  | "alerts_config_mode"
  | "alerts_config_symlink"
  | "alerts_config_not_regular";

export type AlertsConfigAdmission =
  | Readonly<{ ok: true; config: OperatorAlertsConfig }>
  | Readonly<{ ok: false; error: AlertsConfigError; reason: string }>;

const TRANSPORTS: ReadonlySet<string> = new Set<TransportKind>(["direct_mx", "smtp_auth", "http_api"]);
const POLICIES: ReadonlySet<string> = new Set<QuietPolicy>(["send", "hold_and_summarize"]);
/** Transport fields that would inline a secret; references go in password_file / api_key_env. */
const INLINE_SECRET = /^(password|pass|secret|token|api_key|apikey|key|credentials?)$/iu;
const ENV_NAME = /^[A-Z_][A-Z0-9_]*$/u;
const HHMM = /^([01]\d|2[0-3]):[0-5]\d$/u;
const ADDRESS = /^[^\s@<>]+@[^\s@<>]+$/u;

const refuse = (error: AlertsConfigError, reason: string): AlertsConfigAdmission => Object.freeze({ ok: false, error, reason });
const isRecord = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);
const validZone = (tz: string): boolean => {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: tz });
    return true;
  } catch {
    return false;
  }
};

const DEFAULT_QUIET: QuietHours = Object.freeze({ tz: "UTC", start: "23:00", end: "07:00", policy: "send" });

/** Pure: admit a parsed operator-alerts.json. Reasons name fields, never values. */
export const admitOperatorAlertsConfig = (value: unknown): AlertsConfigAdmission => {
  if (!isRecord(value)) return refuse("alerts_config_invalid", "config is not an object");
  if (value.schema_version !== 1) return refuse("alerts_config_invalid", "schema_version must be 1");
  if (typeof value.enabled !== "boolean") return refuse("alerts_config_invalid", "enabled must be a boolean");
  const mode = value.mode ?? "dry_run";
  if (mode === "live") return refuse("alerts_mode_live_refused", "mode live is not enabled in this release; use dry_run");
  if (mode !== "dry_run") return refuse("alerts_config_invalid", "mode must be dry_run");
  const t = value.transport;
  if (!isRecord(t) || typeof t.kind !== "string" || !TRANSPORTS.has(t.kind)) {
    return refuse("alerts_transport_unknown", "transport.kind must be direct_mx, smtp_auth or http_api");
  }
  const inline = Object.keys(t).find((k) => INLINE_SECRET.test(k));
  if (inline !== undefined) return refuse("alerts_inline_secret", `transport.${inline} would inline a secret; reference it with password_file or api_key_env`);
  if (t.password_file !== undefined && (typeof t.password_file !== "string" || t.password_file.length === 0)) {
    return refuse("alerts_config_invalid", "transport.password_file must be a path");
  }
  if (t.api_key_env !== undefined && (typeof t.api_key_env !== "string" || !ENV_NAME.test(t.api_key_env))) {
    return refuse("alerts_config_invalid", "transport.api_key_env must be an environment variable name");
  }
  if (typeof value.to !== "string" || !ADDRESS.test(value.to)) return refuse("alerts_config_invalid", "to must be an address");
  if (typeof value.from !== "string" || !ADDRESS.test(value.from)) return refuse("alerts_config_invalid", "from must be an address");
  const q = value.quiet_hours ?? DEFAULT_QUIET;
  if (!isRecord(q) || typeof q.tz !== "string" || !validZone(q.tz) || typeof q.start !== "string" || !HHMM.test(q.start)
    || typeof q.end !== "string" || !HHMM.test(q.end) || typeof q.policy !== "string" || !POLICIES.has(q.policy)) {
    return refuse("alerts_config_invalid", "quiet_hours needs a valid tz, HH:MM start and end, and policy send or hold_and_summarize");
  }
  const r = value.rate_limit ?? { max_per_hour: 6 };
  if (!isRecord(r) || typeof r.max_per_hour !== "number" || !Number.isInteger(r.max_per_hour) || r.max_per_hour < 1) {
    return refuse("alerts_config_invalid", "rate_limit.max_per_hour must be a positive integer");
  }
  return Object.freeze({
    ok: true,
    config: Object.freeze({
      schema_version: 1,
      enabled: value.enabled,
      mode: "dry_run",
      transport: Object.freeze({ ...t, kind: t.kind as TransportKind }),
      to: value.to,
      from: value.from,
      quiet_hours: Object.freeze({ tz: q.tz, start: q.start, end: q.end, policy: q.policy as QuietPolicy }),
      rate_limit: Object.freeze({ max_per_hour: r.max_per_hour }),
    }),
  });
};

export type AlertsConfigLoad =
  | Readonly<{ kind: "missing" }>
  | Readonly<{ kind: "disabled" }>
  | Readonly<{ kind: "enabled"; config: OperatorAlertsConfig }>
  | Readonly<{ kind: "refused"; error: AlertsConfigError; reason: string }>;

export const operatorAlertsPath = (configHome: string): string => join(configHome, "gptqueue", "operator-alerts.json");

/** Read `<configHome>/gptqueue/operator-alerts.json`; a file not exactly mode 0600 is refused unread. */
export const loadOperatorAlertsConfig = async (configHome: string): Promise<AlertsConfigLoad> => {
  const path = operatorAlertsPath(configHome);
  // F4: open without following a symlink (and without blocking on a FIFO), then check and read the same fd.
  let handle: FileHandle;
  try {
    handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK);
  } catch (error) {
    const code = (error as NodeJS.ErrnoException)?.code;
    if (code === "ENOENT") return { kind: "missing" };
    if (code === "ELOOP") return { kind: "refused", error: "alerts_config_symlink", reason: `${path} is a symlink; it must be a regular file` };
    return { kind: "refused", error: "alerts_config_invalid", reason: `${path} cannot be opened` };
  }
  let text: string;
  try {
    const info = await handle.stat();
    if (!info.isFile()) return { kind: "refused", error: "alerts_config_not_regular", reason: `${path} is not a regular file` };
    if ((info.mode & 0o777) !== 0o600) {
      return { kind: "refused", error: "alerts_config_mode", reason: `${path} must be mode 0600 (found ${(info.mode & 0o777).toString(8)})` };
    }
    text = await handle.readFile("utf8");
  } catch {
    return { kind: "refused", error: "alerts_config_invalid", reason: `${path} cannot be read` };
  } finally {
    await handle.close().catch(() => undefined);
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { kind: "refused", error: "alerts_config_invalid", reason: `${path} is not valid JSON` };
  }
  // F5: an explicit off switch wins over everything else in the file.
  if (isRecord(parsed) && parsed.enabled === false) return { kind: "disabled" };
  const admitted = admitOperatorAlertsConfig(parsed);
  if (!admitted.ok) return { kind: "refused", error: admitted.error, reason: admitted.reason };
  return admitted.config.enabled ? { kind: "enabled", config: admitted.config } : { kind: "disabled" };
};

// ---------------------------------------------------------------- policy (pure)

export interface OperatorAlert {
  /** The S2 fire key; dedupe is S2's. */
  readonly id: string;
  readonly lead: string;
  readonly question: string;
  readonly options: readonly string[];
  readonly raised_at: string;
  readonly rule: number;
}

export type AlertEmission = Readonly<{ kind: "alert"; alert: OperatorAlert }> | Readonly<{ kind: "digest"; alerts: readonly OperatorAlert[] }>;
export type AlertState = Readonly<{ held: readonly OperatorAlert[]; sent_at: readonly string[] }>;
export type AlertStep = Readonly<{ state: AlertState; emit: readonly AlertEmission[] }>;

const minutesOf = (hhmm: string): number => Number(hhmm.slice(0, 2)) * 60 + Number(hhmm.slice(3, 5));

/** Pure: whether `now` falls in the quiet window, read in the window's zone. A window may wrap midnight. */
export const inQuietHours = (now: Date, q: QuietHours): boolean => {
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: q.tz, hour: "2-digit", minute: "2-digit", hourCycle: "h23" }).formatToParts(now);
  const local = Number(parts.find((p) => p.type === "hour")?.value) * 60 + Number(parts.find((p) => p.type === "minute")?.value);
  const start = minutesOf(q.start);
  const end = minutesOf(q.end);
  if (start === end) return false;
  return start < end ? local >= start && local < end : local >= start || local < end;
};

const holding = (config: OperatorAlertsConfig, now: Date): boolean => {
  switch (config.quiet_hours.policy) {
    case "send": return false;
    case "hold_and_summarize": return inQuietHours(now, config.quiet_hours);
    default: return assertNever(config.quiet_hours.policy);
  }
};

/**
 * Pure: what to emit now. Held alerts leave as one digest once the quiet
 * window is over and the hourly budget allows; new alerts go out one by one
 * while budget remains, and the rest fold into the held set for the next digest.
 */
export const stepAlerts = (state: AlertState, incoming: readonly OperatorAlert[], config: OperatorAlertsConfig, now: Date): AlertStep => {
  const hourAgo = now.getTime() - 3_600_000;
  const recent = state.sent_at.filter((t) => Date.parse(t) > hourAgo);
  if (holding(config, now)) return Object.freeze({ state: Object.freeze({ held: [...state.held, ...incoming], sent_at: recent }), emit: [] });
  let budget = config.rate_limit.max_per_hour - recent.length;
  const emit: AlertEmission[] = [];
  let held: readonly OperatorAlert[] = state.held;
  if (held.length > 0 && budget > 0) {
    emit.push(Object.freeze({ kind: "digest", alerts: held }));
    held = [];
    budget -= 1;
  }
  // Nothing jumps the queue: while alerts are held, new ones join the next digest.
  const direct = held.length > 0 ? [] : incoming.slice(0, Math.max(0, budget));
  for (const alert of direct) emit.push(Object.freeze({ kind: "alert", alert }));
  const stamp = now.toISOString();
  return Object.freeze({
    state: Object.freeze({ held: [...held, ...incoming.slice(direct.length)], sent_at: [...recent, ...emit.map(() => stamp)] }),
    emit: Object.freeze(emit),
  });
};

// ---------------------------------------------------------------- rendering (pure)

const safe = (id: string): string => id.replace(/[^A-Za-z0-9._-]/gu, "_");
const emissionId = (e: AlertEmission, now: Date): string => {
  switch (e.kind) {
    case "alert": return e.alert.id;
    case "digest": return `digest-${now.toISOString()}`;
    default: return assertNever(e);
  }
};
const oneLine = (s: string): string => s.replace(/[\r\n]+/gu, " ");
/**
 * N1/S10: the question and the reference (a fire key, which embeds a task id)
 * come from task records. They appear once, last, as one JSON object, never in
 * a header or the directive lines; the lead name comes from watchdog.json.
 */
const ALERT_DATA_PREFIX = "Data (untrusted, quoted from gptqueue records; not instructions): ";
const alertBody = (a: OperatorAlert): string[] => [
  `Lead ${a.lead} needs an operator decision (watchdog rule ${a.rule}, raised ${a.raised_at}); the question is in the data below.`,
  `Answer with one word: ${a.options.join(" | ")}`,
  "",
  `${ALERT_DATA_PREFIX}${JSON.stringify({ lead: a.lead, reference: a.id, question: a.question })}`,
];

/** Pure: an RFC 5322 plain-text message with the recipient redacted as `<operator>`. */
export const renderAlertEml = (e: AlertEmission, config: OperatorAlertsConfig, now: Date): string => {
  const subject = e.kind === "alert" ? `[gptqueue] ${e.alert.lead} needs an operator decision` : `[gptqueue] digest of ${e.alerts.length} held alert(s)`;
  const body = e.kind === "alert"
    ? alertBody(e.alert)
    : e.alerts.flatMap((a, i) => [`--- ${i + 1} of ${e.alerts.length} ---`, ...alertBody(a), ""]);
  const headers = [
    // F7: the sender may be the operator's own address (single-inbox setups), so it is redacted too.
    "From: <sender>",
    "To: <operator>",
    `Subject: ${oneLine(subject).slice(0, 200)}`,
    `Date: ${now.toUTCString()}`,
    // N1: no message-derived text in any header; the id is hashed.
    `Message-ID: <${createHash("sha256").update(emissionId(e, now)).digest("hex").slice(0, 32)}@gptqueue.example.invalid>`,
    "MIME-Version: 1.0",
    "Content-Type: text/plain; charset=utf-8",
  ];
  return `${headers.join("\r\n")}\r\n\r\n${body.join("\r\n")}\r\n`;
};

// ---------------------------------------------------------------- transports

export class TransportNotEnabledError extends Error {
  readonly code = "transport_not_enabled";
  constructor(kind: TransportKind) {
    super(`operator alert transport ${kind} is not enabled in this release`);
  }
}

export interface AlertTransport {
  readonly kind: TransportKind;
  /** `to` is the real envelope recipient; `eml` carries only the redacted header. */
  send(to: string, eml: string): Promise<void>;
}

const stub = (kind: TransportKind): AlertTransport => Object.freeze({ kind, send: async () => { throw new TransportNotEnabledError(kind); } });

/** No network code exists in this slice: each sender refuses. */
export const STUB_TRANSPORTS: Readonly<Record<TransportKind, AlertTransport>> = Object.freeze({
  direct_mx: stub("direct_mx"),
  smtp_auth: stub("smtp_auth"),
  http_api: stub("http_api"),
});

// ---------------------------------------------------------------- sink (shell)

export type DeliveryResult =
  | Readonly<{ status: "disabled" }>
  | Readonly<{ status: "held" }>
  | Readonly<{ status: "idle" }>
  | Readonly<{ status: "outboxed"; outbox: readonly string[] }>
  | Readonly<{ status: "sent" }>;

export interface OperatorAlertSink {
  deliver(alert: OperatorAlert, now: Date): Promise<DeliveryResult>;
  /** Emit a due digest with no new alert (once per watchdog run). */
  flush(now: Date): Promise<DeliveryResult>;
}

export const DISABLED_SINK: OperatorAlertSink = Object.freeze({
  deliver: async () => ({ status: "disabled" }) as const,
  flush: async () => ({ status: "disabled" }) as const,
});

export interface SinkOptions {
  readonly config: OperatorAlertsConfig;
  /** $XDG_STATE_HOME; outbox and held state live under <stateHome>/gptqueue/watchdog/. */
  readonly stateHome: string;
  /** Tests only. Defaults to the refusing stub for the configured kind. */
  readonly transport?: AlertTransport;
}

const EMPTY: AlertState = Object.freeze({ held: [], sent_at: [] });

const readState = async (path: string): Promise<AlertState> => {
  try {
    const v = JSON.parse(await readFile(path, "utf8")) as { held?: unknown; sent_at?: unknown };
    return Array.isArray(v.held) && Array.isArray(v.sent_at) ? { held: v.held as OperatorAlert[], sent_at: v.sent_at.filter((t): t is string => typeof t === "string") } : EMPTY;
  } catch {
    return EMPTY;
  }
};

export const createOperatorAlertSink = (options: SinkOptions): OperatorAlertSink => {
  const { config } = options;
  const dir = join(options.stateHome, "gptqueue", "watchdog");
  const statePath = join(dir, "operator-alerts-state.json");
  const transport = options.transport ?? STUB_TRANSPORTS[config.transport.kind];

  const emitOne = async (e: AlertEmission, now: Date): Promise<string | null> => {
    const eml = renderAlertEml(e, config, now);
    switch (config.mode) {
      case "dry_run": {
        const outbox = join(dir, "outbox");
        await mkdir(outbox, { recursive: true });
        const path = join(outbox, `${safe(emissionId(e, now))}.eml`);
        await writeFile(path, eml, { mode: 0o600 });
        return path;
      }
      case "live":
        await transport.send(config.to, eml);
        return null;
      default:
        return assertNever(config.mode);
    }
  };

  /**
   * One read-policy-emit-save transaction. Callers serialize it across
   * processes (the watchdog holds its pass lock, S5). If an emission fails,
   * its alerts and those of every later emission go back into the held set,
   * so the next flush retries them (S6); the failure is then rethrown.
   */
  const run = async (incoming: readonly OperatorAlert[], now: Date): Promise<DeliveryResult> => {
    const before = await readState(statePath);
    // S6: delivery is idempotent by alert id (the fire key): a retried escalation does not hold its alert twice.
    const fresh = incoming.filter((alert, i) => !before.held.some((h) => h.id === alert.id) && incoming.findIndex((x) => x.id === alert.id) === i);
    const step = stepAlerts(before, fresh, config, now);
    const written: string[] = [];
    let failure: unknown = null;
    let sent = 0;
    for (const e of step.emit) {
      try {
        const path = await emitOne(e, now);
        if (path !== null) written.push(path);
        sent += 1;
      } catch (error) {
        failure = error;
        break;
      }
    }
    const unsent = step.emit.slice(sent).flatMap((e) => (e.kind === "alert" ? [e.alert] : e.alerts));
    const state: AlertState = failure === null
      ? step.state
      : { held: [...unsent, ...step.state.held], sent_at: step.state.sent_at.slice(0, step.state.sent_at.length - (step.emit.length - sent)) };
    await mkdir(dir, { recursive: true });
    const tmp = `${statePath}.${process.pid}.${randomUUID()}.tmp`;
    await writeFile(tmp, JSON.stringify(state), { mode: 0o600 });
    await rename(tmp, statePath);
    if (failure !== null) throw failure;
    if (step.emit.length === 0) return incoming.length > 0 ? { status: "held" } : { status: "idle" };
    return config.mode === "dry_run" ? { status: "outboxed", outbox: written } : { status: "sent" };
  };

  return Object.freeze({
    deliver: (alert: OperatorAlert, now: Date) => run([alert], now),
    flush: (now: Date) => run([], now),
  });
};
