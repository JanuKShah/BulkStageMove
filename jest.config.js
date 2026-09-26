/**
 * Jest is split into two projects because the two kinds of test have different
 * requirements.
 *
 *   test/safety      one test per safety mechanism. These are the ones that go
 *                    red if a guard, constraint or check is removed.
 *   test/endpoints   full coverage of every route and its failure paths.
 *
 * The safety tests talk to Postgres directly, so DATABASE_URL must resolve and
 * the compose stack must be up. They fail loudly if it is not - never skip,
 * because a silently skipped safety test is worse than no test.
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
    { ...common, displayName: 'safety', testMatch: ['<rootDir>/test/safety/**/*.spec.ts'] },
    { ...common, displayName: 'endpoints', testMatch: ['<rootDir>/test/endpoints/**/*.spec.ts'] },
  ],
};
