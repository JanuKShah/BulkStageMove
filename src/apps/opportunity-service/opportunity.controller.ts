import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { requireUuid, requireWorkspaceId } from '../../shared/tenancy/workspace-guard';
import { Opportunity, Transition } from './opportunity.repository';
import { OpportunityService } from './opportunity.service';

type Headers_ = Record<string, string | string[] | undefined>;

/** Matches PAGE_SIZE in transition-service, so a batch is one request. */
const MAX_BULK_IDS = 1000;

@Controller('opportunities')
export class OpportunityController {
  constructor(private readonly service: OpportunityService) {}

  @Post()
  create(
    @Headers() headers: Headers_,
    @Body() body: { stageId?: string; name?: string; value?: number; ownerId?: string },
  ): Promise<Opportunity> {
    return this.service.create(requireWorkspaceId(headers), body);
  }

  @Get()
  list(
    @Headers() headers: Headers_,
    @Query() query: Record<string, unknown>,
  ): Promise<{ items: Opportunity[]; nextCursor: string | null }> {
    return this.service.list(requireWorkspaceId(headers), query);
  }

  @Get(':id/transitions')
  transitions(@Headers() headers: Headers_, @Param('id') id: string): Promise<Transition[]> {
    return this.service.transitions(requireWorkspaceId(headers), requireUuid('id', id));
  }

  /**
   * One request per batch of a bulk job. Separate from :id/move because it
   * resolves the permitted targets once per source stage and writes set-based,
   * which is what makes 1000 records one transaction instead of two thousand
   * statements and a thousand rule lookups.
   */
  @Post('bulk-move')
  @HttpCode(200)
  async bulkMove(
    @Headers() headers: Headers_,
    @Body() body: { ids?: string[]; toStageId?: string; jobId?: string },
  ): Promise<{ moved: number; refused: number; missing: number }> {
    const workspaceId = requireWorkspaceId(headers);
    const { ids, toStageId, jobId } = body;
    if (!Array.isArray(ids) || ids.length === 0) {
      throw new BadRequestException('ids must be a non-empty array');
    }
    if (ids.length > MAX_BULK_IDS) {
      throw new BadRequestException(`ids is limited to ${MAX_BULK_IDS} per request`);
    }
    for (const id of ids) requireUuid('ids', String(id));
    if (!toStageId) throw new BadRequestException('toStageId is required');
    requireUuid('toStageId', toStageId);
    if (jobId !== undefined) requireUuid('jobId', String(jobId));

    const result = await this.service.bulkMove(
      workspaceId,
      ids.map(String),
      toStageId,
      jobId ?? null,
    );
    return {
      moved: result.moved.length,
      refused: result.refused.length,
      missing: result.missing.length,
    };
  }

  // A move mutates an existing opportunity; it does not create one. 201 would
  // tell the client a new resource now exists at this URI.
  @Post(':id/move')
  @HttpCode(200)
  move(
    @Headers() headers: Headers_,
    @Param('id') id: string,
    @Body() body: { toStageId?: string },
  ): Promise<Opportunity> {
    const toStageId = body.toStageId;
    if (!toStageId) throw new BadRequestException('toStageId is required');
    return this.service.move(
      requireWorkspaceId(headers),
      requireUuid('id', id),
      requireUuid('toStageId', toStageId),
    );
  }
}
