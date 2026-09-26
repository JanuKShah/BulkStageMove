/**
 * Times each jest project over repeated runs and reports the distribution.
 *
 *   npx tsx test/happyflow/timing.ts [runs]
 *
 * Per project rather than one blended average of `npm test`. A single mean over
 * all three would be dominated by whichever is slowest - the happy-flow suite
 * builds a 52,000 row fixture and moves 50,000 records through the broker on
 * every run - and would hide that the other two finish in seconds.
 *
 * Reports median and spread, not just a mean, because one slow run in twenty is
 * the interesting datum: it is either a cold start or a flake.
 */
import { execFileSync } from 'node:child_process';

const RUNS = Number(process.argv[2] ?? 20);
/** happyflow moves 50,000 records per run, so it gets fewer by default. */
const PLANNED: Record<string, number> = { safety: RUNS, endpoints: RUNS, happyflow: 3 };

interface Sample {
  seconds: number;
  passed: number;
  failed: number;
  ok: boolean;
}

function runOnce(project: string): Sample {
  const started = process.hrtime.bigint();
  let out = '';
  let ok = true;
  try {
    out = execFileSync('npx', ['jest', '--runInBand', '--selectProjects', project], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
      maxBuffer: 32 * 1024 * 1024,
    });
  } catch (error) {
    out = `${(error as { stdout?: string }).stdout ?? ''}`;
    ok = false;
  }
  const seconds = Number(process.hrtime.bigint() - started) / 1e9;
  const tests = /^Tests:\s+(?:(\d+) failed, )?(?:(\d+) passed, )?(\d+) total/m.exec(out);
  return {
    seconds,
    failed: Number(tests?.[1] ?? 0),
    passed: Number(tests?.[2] ?? 0),
    total: Number(tests?.[3] ?? 0),
    ok,
  } as Sample;
}

function pct(sorted: number[], p: number): number {
  if (sorted.length === 0) return 0;
  return sorted[Math.min(Math.ceil((p / 100) * sorted.length), sorted.length) - 1]!;
}

function main(): void {
  console.log(`timing each project over repeated runs on this machine\n`);
  for (const [project, runs] of Object.entries(PLANNED)) {
    const samples: Sample[] = [];
    process.stdout.write(`  ${project} (${runs} runs): `);
    for (let i = 0; i < runs; i++) {
      const s = runOnce(project);
      samples.push(s);
      process.stdout.write(s.ok ? '.' : 'F');
    }
    console.log();

    const times = samples.map((s) => s.seconds).sort((a, b) => a - b);
    const total = times.reduce((a, b) => a + b, 0);
    const last = samples[samples.length - 1]!;
    console.log(`    tests        ${last.passed} passed, ${last.failed} failed`);
    console.log(`    min / median ${pct(times, 0).toFixed(1)}s / ${pct(times, 50).toFixed(1)}s`);
    console.log(`    p95 / max    ${pct(times, 95).toFixed(1)}s / ${times[times.length - 1]!.toFixed(1)}s`);
    console.log(`    mean         ${(total / times.length).toFixed(1)}s`);
    console.log();
  }
}

main();
