// Temporary renderer profiling harness (never merges) — event-loop drift math.
//
// WebKit exposes no Long Tasks API, so main-thread stalls are inferred from
// timer drift: a timer armed for `intervalMs` that fires late by more than
// `thresholdMs` means the main thread was blocked for roughly that overage.

/** Default sampling interval for the drift timer. */
export const DRIFT_INTERVAL_MS = 500;
/** Report a stall only when drift exceeds this — filters normal scheduler jitter. */
export const DRIFT_THRESHOLD_MS = 50;

/**
 * Overage of an observed timer fire versus its expected fire time.
 *
 * `expected` is `armedAt + intervalMs`; `observed` is the actual fire time.
 * Returns the non-negative main-thread block estimate, or `null` when the
 * fire is within threshold (no stall worth recording).
 */
export function computeDrift(
  observed: number,
  expected: number,
  thresholdMs: number = DRIFT_THRESHOLD_MS,
): number | null {
  const drift = observed - expected;
  return drift > thresholdMs ? drift : null;
}
