import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';

export interface Stage {
  id: string;
  workspace_id: string;
  name: string;
  outcome: string;
}

const COLUMNS = 'id, workspace_id, name, outcome';

@Injectable()
export class StageRepository {
  constructor(private readonly db: DatabaseService) {}

  async list(workspaceId: string): Promise<Stage[]> {
    return this.db.query<Stage>(
      `SELECT ${COLUMNS} FROM stage WHERE workspace_id = $1 ORDER BY name`,
      [workspaceId],
    );
  }

  async findById(workspaceId: string, id: string): Promise<Stage | null> {
    const rows = await this.db.query<Stage>(
      `SELECT ${COLUMNS} FROM stage WHERE id = $1 AND workspace_id = $2`,
      [id, workspaceId],
    );
    return rows[0] ?? null;
  }

  async isMoveAllowed(workspaceId: string, from: string, to: string): Promise<boolean> {
    const rows = await this.db.query<{ allowed: boolean }>(
      `SELECT EXISTS (
         SELECT 1 FROM stage_transition_rule
         WHERE workspace_id = $1 AND from_stage_id = $2 AND to_stage_id = $3
       ) AS allowed`,
      [workspaceId, from, to],
    );
    return rows[0]?.allowed ?? false;
  }

  async allowedTargets(workspaceId: string, from: string): Promise<string[]> {
    const rows = await this.db.query<{ to_stage_id: string }>(
      `SELECT to_stage_id FROM stage_transition_rule
       WHERE workspace_id = $1 AND from_stage_id = $2`,
      [workspaceId, from],
    );
    return rows.map((r) => r.to_stage_id);
  }
}
