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
