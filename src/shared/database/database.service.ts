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
    });
  }

  async query<T>(text: string, values: unknown[] = []): Promise<T[]> {
    const result = await this.pool.query(text, values);
    return result.rows as T[];
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

  async ping(): Promise<void> {
    await this.pool.query('SELECT 1');
  }

  async onModuleDestroy(): Promise<void> {
    await this.pool.end();
  }
}
