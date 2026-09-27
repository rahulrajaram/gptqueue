import { isAbsolute } from "node:path";
import { z } from "zod";

/** Runtime identity is supplied by the owning host, never inferred from cwd. */
export const runtimeBindingSchema = z.object({
  client: z.enum(["codex", "pi"]),
  runtime_id: z.string().min(1).max(200),
  epoch: z.string().min(1).max(200),
  working_directory: z.string().refine(isAbsolute, "An absolute directory is required"),
}).strict();

export type RuntimeBinding = Readonly<z.infer<typeof runtimeBindingSchema>>;
