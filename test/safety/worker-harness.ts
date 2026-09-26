/**
 * Calls the batch worker directly, without the broker.
 *
 * The worker tests otherwise have to go through RabbitMQ and wait for a container
 * to pick the message up, which cannot express "run the same batch twice" - the
 * whole point of a retry test. Constructing the repository by hand is enough,
 * because both it and its dependencies are plain classes whose only injections
 * are a pool and a config object.
 *
 * This is the only way to test retry semantics deterministically, so it is worth
 * having even though it reaches past the Nest container.
 */
import { WorkerRepository } from '../../src/apps/worker-service/worker.repository';
import { DatabaseService } from '../../src/shared/database/database.service';
import { rabbitConfig } from '../../src/shared/rabbit/rabbit.config';
import { DB_URL } from '../helpers';

export interface BatchOutcome {
  disposition: string;
  moved: number;
  failed: number;
  skipped: number;
  attempts: number;
}

let db: DatabaseService | null = null;
let repository: WorkerRepository | null = null;

export async function processBatchForTest(
  workspaceId: string,
  jobId: string,
  batchNo: number,
): Promise<BatchOutcome> {
  if (!repository) {
    // DatabaseService reads DATABASE_URL from the environment, which the test
    // process does not set - it resolves the same URL the helper pool uses.
    process.env.DATABASE_URL = DB_URL;
    db = new DatabaseService();
    repository = new WorkerRepository(db, rabbitConfig());
  }
  return repository.processBatch(workspaceId, jobId, batchNo);
}

/**
 * Ends the pool this harness opened.
 *
 * Without it the pool's sockets stay open after the suite and Jest reports
 * "did not exit one second after the test run has completed" and hangs.
 */
export async function closeWorkerHarness(): Promise<void> {
  if (db) {
    await db.onModuleDestroy();
    db = null;
    repository = null;
  }
}
