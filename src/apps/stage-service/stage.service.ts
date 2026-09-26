import { Injectable, NotFoundException } from '@nestjs/common';
import { Stage, StageRepository } from './stage.repository';

@Injectable()
export class StageService {
  constructor(private readonly repository: StageRepository) {}

  list(workspaceId: string): Promise<Stage[]> {
    return this.repository.list(workspaceId);
  }

  async getById(workspaceId: string, id: string): Promise<Stage> {
    const found = await this.repository.findById(workspaceId, id);
    if (!found) throw new NotFoundException(`stage ${id} not found`);
    return found;
  }

  /**
   * Whether a move is permitted. Absence of a stage_transition_rule row is what
   * makes a state non-transitionable.
   */
  isMoveAllowed(workspaceId: string, from: string, to: string): Promise<boolean> {
    return this.repository.isMoveAllowed(workspaceId, from, to);
  }

  /**
   * Bulk form of the rule lookup. The bulk job must never validate per record -
   * 50k HTTP round trips is untenable and would make this service a hard
   * availability dependency for every bulk move. Callers resolve the permitted
   * targets once, then apply in bulk.
   */
  allowedTargets(workspaceId: string, from: string): Promise<string[]> {
    return this.repository.allowedTargets(workspaceId, from);
  }
}
