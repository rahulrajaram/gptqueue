import { routeIds, type HostFamily, type RouteId, type RouteSpec } from "./qualification-types.js";
import type { ExpectedDimensionObligation } from "./qualification-report.js";

const hostOf = (id: RouteId): HostFamily => id.split("-", 1)[0] as HostFamily;
const modelBacked = (id: RouteId): boolean => !id.startsWith("generic-");

export const frozenRoutes: readonly RouteSpec[] = Object.freeze(routeIds.map((id) => Object.freeze({
  id, host: hostOf(id), modelBacked: modelBacked(id), availability: { kind: "setup_gap", detail: "preflight not run" } as const,
})));

export const orderedPairMatrix = (routes: readonly RouteSpec[]) => Object.freeze(
  routes.flatMap((sender) => routes.map((receiver) => Object.freeze({
    pairId: `${sender.id}->${receiver.id}`,
    sender: sender.id,
    receiver: receiver.id,
  }))),
);

export const frozenPairMatrix = orderedPairMatrix(frozenRoutes);

/** Explicit non-pair obligations; generic clients have no model behavior requirement. */
export const frozenDimensionObligations: readonly ExpectedDimensionObligation[] = Object.freeze(
  frozenRoutes.flatMap((route) => [
    ...["idle-task", "idle-result", "idle-error", "busy-deferral", "readable-ping", "readable-status"].map((trial) => Object.freeze({
      id: `${route.id}:automatic:${trial}`, kind: "automatic_tasks" as const, route: route.id, required: route.modelBacked,
    })),
    ...[1, 2, 3].map((trial) => Object.freeze({
      id: `${route.id}:initiative:${trial}`, kind: "initiative" as const, route: route.id, required: route.modelBacked,
    })),
    Object.freeze({ id: `${route.id}:registration:unnamed`, kind: "registration" as const, route: route.id, required: route.modelBacked }),
  ]),
);

export const pairKey = (sender: RouteId, receiver: RouteId): string => `${sender}->${receiver}`;
