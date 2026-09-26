import { existsSync } from 'node:fs';
import path from 'node:path';

/**
 * Config resolution for the scripts.
 *
 * A .env file is honoured when present, using node's built-in loader - no dotenv
 * dependency. Existing environment variables always win, so CI and compose can
 * override without editing a file.
 *
 * The fallback is the local compose database. Without it the brief's "one
 * command on a clean machine" fails, because a fresh clone has no .env and the
 * scripts would have nothing to connect to.
 */
const envFile = path.join(__dirname, '..', '.env');
if (existsSync(envFile)) process.loadEnvFile(envFile);

export const DEFAULT_DATABASE_URL =
  'postgres://app:app_dev_password@localhost:5432/bulk_stage_move';

export function databaseUrl(): string {
  return process.env.DATABASE_URL ?? DEFAULT_DATABASE_URL;
}
