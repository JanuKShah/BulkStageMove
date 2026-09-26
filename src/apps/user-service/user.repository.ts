import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';

export interface User {
  id: string;
  workspace_id: string;
  name: string;
  email: string | null;
}

const COLUMNS = 'id, workspace_id, name, email';

@Injectable()
export class UserRepository {
  constructor(private readonly db: DatabaseService) {}

  async create(workspaceId: string, name: string, email: string | null): Promise<User> {
    const rows = await this.db.query<User>(
      `INSERT INTO app_user (workspace_id, name, email) VALUES ($1, $2, $3)
       RETURNING ${COLUMNS}`,
      [workspaceId, name, email],
    );
    return rows[0]!;
  }

  async list(workspaceId: string, limit: number): Promise<User[]> {
    return this.db.query<User>(
      `SELECT ${COLUMNS} FROM app_user WHERE workspace_id = $1 ORDER BY name LIMIT $2`,
      [workspaceId, limit],
    );
  }

  async findById(workspaceId: string, id: string): Promise<User | null> {
    const rows = await this.db.query<User>(
      `SELECT ${COLUMNS} FROM app_user WHERE id = $1 AND workspace_id = $2`,
      [id, workspaceId],
    );
    return rows[0] ?? null;
  }
}
