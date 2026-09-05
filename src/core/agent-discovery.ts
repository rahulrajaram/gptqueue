/** Public identity metadata stored alongside a queue registration. */
export type AgentDiscoveryMetadata = Readonly<{
  label: string;
  uuid: string | null;
  client: "codex" | "pi" | null;
  working_directory: string | null;
}>;

export type AgentDiscoveryRecord = Readonly<AgentDiscoveryMetadata & {
  name: string;
  role: string;
  description?: string;
  online: boolean;
  registered_at: string | null;
  pid: number | null;
}>;

const nonblank = (value: unknown): string | null =>
  typeof value === "string" && value.trim().length > 0 ? value.trim() : null;

const uuid = (value: unknown): string | null =>
  typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu.test(value)
    ? value : null;

const client = (value: unknown): "codex" | "pi" | null =>
  value === "codex" || value === "pi" ? value : null;

const directory = (value: unknown): string | null =>
  typeof value === "string" && value.startsWith("/") ? value : null;

export const discoveryMetadata = (
  name: string,
  metadata?: unknown
): AgentDiscoveryMetadata => {
  const values = metadata && typeof metadata === "object" && !Array.isArray(metadata)
    ? metadata as Record<string, unknown> : {};
  return Object.freeze({
    label: nonblank(values.label) ?? name,
    uuid: uuid(values.uuid),
    client: client(values.client),
    working_directory: directory(values.working_directory),
  });
};

export const discoveryRecord = (input: {
  name: string; role: string; description?: string; online: boolean;
  registered_at?: unknown; pid?: unknown; metadata?: unknown;
}): AgentDiscoveryRecord => Object.freeze({
  name: input.name,
  role: input.role,
  description: input.description,
  online: input.online,
  ...discoveryMetadata(input.name, input.metadata),
  registered_at: typeof input.registered_at === "string" && !Number.isNaN(Date.parse(input.registered_at)) ? input.registered_at : null,
  pid: typeof input.pid === "number" && Number.isSafeInteger(input.pid) && input.pid > 0 ? input.pid : null,
});
