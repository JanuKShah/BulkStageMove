import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { DatabaseService } from '../../shared/database/database.service';
import { ServiceClient } from '../../shared/http/service-client';
import { parseListFilter, type Outcome } from './opportunity.filter';
import { Opportunity, OpportunityRepository, Transition } from './opportunity.repository';

export interface CreateInput {
  stageId?: string | undefined;
  name?: string | undefined;
  value?: number | undefined;
  ownerId?: string | undefined;
}

@Injectable()
export class OpportunityService {
  constructor(
    private readonly repository: OpportunityRepository,
    private readonly database: DatabaseService,
    private readonly stages: ServiceClient,
  ) {}

  /**
   * The insert and its opening transition row are one transaction. A transition
   * lost to a partial write means a permanently broken audit trail.
   */
  async create(workspaceId: string, input: CreateInput): Promise<Opportunity> {
    const name = input.name?.trim();
    if (!name) throw new BadRequestException('name is required');
    if (!input.stageId) throw new BadRequestException('stageId is required');

    return this.database.transaction(async (client) => {
      const created = await this.repository.insert(client, {
        id: randomUUID(),
        workspaceId,
        stageId: input.stageId!,
        name,
        value: input.value ?? 0,
        ownerId: input.ownerId ?? null,
      });
      await this.repository.insertTransition(client, {
        workspaceId,
        opportunityId: created.id,
        from: null,
        to: input.stageId!,
      });
      return created;
    });
  }

  /**
   * Move to another stage. The stage update and the transition record are one
   * transaction - either both land or neither does.
   *
   * The permitted-move check is a call to stage-service, which owns the rules.
   * It is not cached locally: correctness first, and the bulk path avoids
   * per-record calls entirely by resolving the permitted targets once.
   */
  async move(workspaceId: string, id: string, toStageId: string): Promise<Opportunity> {
    const current = await this.getById(workspaceId, id);

    const { allowed } = await this.stages.post<{ allowed: boolean }>(
      'stage',
      '/stages/can-move',
      workspaceId,
      { from: current.stage_id, to: toStageId },
    );
    if (!allowed) {
      throw new ConflictException(`move from ${current.stage_id} to ${toStageId} is not allowed`);
    }

    return this.database.transaction(async (client) => {
      const moved = await this.repository.updateStage(client, workspaceId, id, toStageId);
      if (!moved) throw new NotFoundException(`opportunity ${id} not found`);
      await this.repository.insertTransition(client, {
        workspaceId,
        opportunityId: id,
        from: current.stage_id,
        to: toStageId,
      });
      return moved;
    });
  }

  /**
   * Resolve an outcome filter to the stage ids carrying it. One call to
   * stage-service per request, never per record - the bulk job filters on these
   * same dimensions across tens of thousands of rows, so per-row resolution
   * would make stage-service a hard dependency of every list and every job.
   */
  private async stageIdsForOutcome(workspaceId: string, outcome: Outcome): Promise<string[]> {
    const stages = await this.stages.get<{ id: string; outcome: string }[]>(
      'stage',
      '/stages',
      workspaceId,
    );
    return stages.filter((s) => s.outcome === outcome).map((s) => s.id);
  }

  async list(
    workspaceId: string,
    query: Record<string, unknown>,
  ): Promise<{ items: Opportunity[]; nextCursor: string | null }> {
    const filter = parseListFilter(query);

    if (filter.outcome !== undefined) {
      filter.stageIds = await this.stageIdsForOutcome(workspaceId, filter.outcome);
    }

    const { items, hasMore } = await this.repository.list(workspaceId, filter);
    return { items, nextCursor: hasMore ? (items.at(-1)?.id ?? null) : null };
  }

  transitions(workspaceId: string, opportunityId: string): Promise<Transition[]> {
    return this.repository.listTransitions(workspaceId, opportunityId);
  }

  private async getById(workspaceId: string, id: string): Promise<Opportunity> {
    const found = await this.repository.findById(workspaceId, id);
    if (!found) throw new NotFoundException(`opportunity ${id} not found`);
    return found;
  }
}
