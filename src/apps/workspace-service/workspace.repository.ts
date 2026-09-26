import { Injectable } from '@nestjs/common';
import { DatabaseService } from '../../shared/database/database.service';

export interface Workspace {
  id: string;
  name: string;
  created_at: Date;
}

@Injectable()
export class WorkspaceRepository {
  constructor(private readonly db: DatabaseService) {}

  async create(name: string): Promise<Workspace> {
    const rows = await this.db.query<Workspace>(
      'INSERT INTO workspace (name) VALUES ($1) RETURNING id, name, created_at',
      [name],
    );
    return rows[0]!;
  }

  async findById(id: string): Promise<Workspace | null> {
    const rows = await this.db.query<Workspace>(
      'SELECT id, name, created_at FROM workspace WHERE id = $1',
      [id],
    );
    return rows[0] ?? null;
  }
}
