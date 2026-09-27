/**
 * Jest is split into four projects because the four kinds of test have
 * different requirements.
 *
 *   test/unit        pure functions, no database and no stack. Separate so they
 *                    stay runnable on their own: a test that needs the compose
 *                    stack to answer "does this string parser handle a quote"
 *                    is a test that goes unexplained for the wrong reason.
 *   test/safety      one test per safety mechanism. These are the ones that go
 *                    red if a guard, constraint or check is removed.
 *   test/endpoints   full coverage of every route and its failure paths.
 *   test/happyflow   the whole brief at full scale: one 50,000 record job, end
 *                    to end through the broker. Slow by nature, so it runs in
 *                    band and is given a long timeout. Correctness only - the
 *                    timings live in benchmark.ts, because a latency assertion
 *                    fails on a loaded machine and passes on a fast one.
 *
 * The safety and happyflow tests talk to Postgres directly, so DATABASE_URL
 * must resolve and the compose stack must be up. They fail loudly if it is not -
 * never skip, because a silently skipped safety test is worse than no test.
 */
const transform = {
  '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json' }],
};

const common = {
  rootDir: '.',
  transform,
  testEnvironment: 'node',
  testTimeout: 30_000,
};

module.exports = {
  projects: [
    { ...common, displayName: 'unit', testMatch: ['<rootDir>/test/unit/**/*.spec.ts'] },
    { ...common, displayName: 'safety', testMatch: ['<rootDir>/test/safety/**/*.spec.ts'] },
    { ...common, displayName: 'endpoints', testMatch: ['<rootDir>/test/endpoints/**/*.spec.ts'] },
    {
      ...common,
      displayName: 'happyflow',
      testMatch: ['<rootDir>/test/happyflow/**/*.spec.ts'],
      testTimeout: 900_000,
    },
  ],
};
