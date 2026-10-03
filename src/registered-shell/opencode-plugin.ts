import {
  createOpenCodePlugin,
  type OpenCodePluginInput,
  type OpenCodePluginHooks,
} from "./opencode-plugin-factory.js";

/** OpenCode plugin entrypoint: keep this module's runtime exports default-only. */
export default async function gptqueueOpenCodePlugin(
  input: OpenCodePluginInput,
  rawOptions: Record<string, unknown> = {},
): Promise<OpenCodePluginHooks> {
  const redisUrl = typeof rawOptions.redisUrl === "string"
    ? rawOptions.redisUrl
    : process.env.GPTQUEUE_REDIS_URL ?? process.env.REDIS_URL;
  if (!redisUrl) {
    throw new Error("GPTQueue OpenCode plugin requires redisUrl, GPTQUEUE_REDIS_URL, or REDIS_URL");
  }
  const epoch = typeof rawOptions.epoch === "string" ? rawOptions.epoch : undefined;
  const timeoutMs = typeof rawOptions.timeoutMs === "number" ? rawOptions.timeoutMs : undefined;
  const intervalMs = typeof rawOptions.intervalMs === "number" ? rawOptions.intervalMs : undefined;
  const maxAttempts = typeof rawOptions.maxAttempts === "number" ? rawOptions.maxAttempts : undefined;
  return createOpenCodePlugin(input, {
    redisUrl,
    epoch,
    runtimeOptions: timeoutMs === undefined ? undefined : { timeoutMs },
    dispatcherOptions: intervalMs === undefined && maxAttempts === undefined
      ? undefined : { intervalMs, maxAttempts },
  });
}
