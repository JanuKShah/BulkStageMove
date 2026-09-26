import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { Workspace, WorkspaceRepository } from './workspace.repository';

@Injectable()
export class WorkspaceService {
  constructor(private readonly repository: WorkspaceRepository) {}

  async create(name: string | undefined): Promise<Workspace> {
    const trimmed = name?.trim();
    if (!trimmed) throw new BadRequestException('name is required');
    return this.repository.create(trimmed);
  }

  async getById(id: string): Promise<Workspace> {
    const found = await this.repository.findById(id);
    if (!found) throw new NotFoundException(`workspace ${id} not found`);
    return found;
  }
}
