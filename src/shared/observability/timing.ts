import { hrtime } from 'node:process';

/**
 * Monotonic elapsed time for log lines and timing breakdowns.
 *
 * hrtime rather than Date.now, because a duration computed from the wall clock
 * can come out negative when the clock is adjusted mid-run, and a benchmark that
 * reports -4ms is worse than one that reports nothing. It is also not subject to
 * the resolution coarseness that makes Date.now() report 0 for two events in the
 * same millisecond, which at batch granularity is most of them.
 */
export function elapsedMs(started: bigint): number {
  return Number(hrtime.bigint() - started) / 1e6;
}

/** Starts a stopwatch. Pass the result to elapsedMs. */
export function startTimer(): bigint {
  return hrtime.bigint();
}

/**
 * One decimal place, for log lines.
 *
 * Sub-millisecond precision is noise at this scale: a batch is hundreds of
 * milliseconds, and a third digit invites reading a difference into two runs
 * that differ only by scheduling.
 */
export function ms1(value: number): string {
  return `${value.toFixed(1)}ms`;
}
