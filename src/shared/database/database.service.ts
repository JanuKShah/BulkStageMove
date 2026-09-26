import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { Pool, type PoolClient } from 'pg';

/**
 * Postgres access. Each microservice process gets its own pool, so PG_POOL_MAX
 * is per service rather than shared.
 *
 * Datastores are shared across services - one Postgres, one schema - with table
 * ownership by convention rather than a database per service. The reason is the
 * composite foreign keys that enforce tenant isolation: an opportunity carries
 * workspace_id and references stage(id, workspace_id). With stage in a separate
 * database that key could not exist and isolation would degrade into an
 * application-level check.
 */
@Injectable()
export class DatabaseService implements OnModuleDestroy {
  readonly pool: Pool;

  constructor() {
    this.pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      max: Number(process.env.PG_POOL_MAX ?? 10),
      // Without this every connection is anonymous in pg_stat_activity, so there
      // is no way to tell which of the services is holding connections or
      // running the slow query. One env var per service makes that answerable.
      application_name: process.env.PG_APP_NAME ?? 'bulk-stage-move',
    });
  }

  async query<T>(text: string, values: unknown[] = []): Promise<T[]> {
    const result = await this.pool.query(text, values);
    return result.rows as T[];
  }

  /**
   * A single connection, held by the caller.
   *
   * Needed for anything session-scoped - notably advisory locks, which bind to
   * the connection rather than the transaction. Taking one through query() would
   * put it on an arbitrary pooled connection, so a second worker could acquire
   * the same lock on a different connection, and the first connection would go
   * back to the pool still holding it and that key could never be locked again.
   */
  async connect(): Promise<PoolClient> {
    return this.pool.connect();
  }

  /**
   * Runs fn inside a transaction, committing on success and rolling back on any
   * throw. Transaction boundaries belong here so services express intent
   * ("these writes are atomic") rather than repeating BEGIN/COMMIT plumbing.
   */
  async transaction<T>(fn: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  /**
   * Readiness check. It touches a real table rather than running a bare
   * SELECT 1, because SELECT 1 succeeds against an empty database and would
   * report a freshly started stack as healthy while every endpoint returns 500
   * for a missing schema. This fails until migrations have run.
   */
  async ping(): Promise<void> {
    await this.pool.query('SELECT 1 FROM workspace LIMIT 1');
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
