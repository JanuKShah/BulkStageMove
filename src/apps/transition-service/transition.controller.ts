import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { requireUuid, requireWorkspaceId } from '../../shared/tenancy/workspace-guard';
import { TransitionService } from './transition.service';

type Headers_ = Record<string, string | string[] | undefined>;

@Controller('bulk-moves')
export class TransitionController {
  constructor(private readonly service: TransitionService) {}

  /**
   * Always 201, including when the key replays an existing job: a replay is not a
   * lesser success, the job exists and the client is holding it. A status that
   * changed between the original and the retry would leave the contract only
   * half idempotent. The body reports which via `replay`, so the two facts are
   * never carried by channels that can disagree.
   */
  @Post()
  async submit(
    @Headers() headers: Headers_,
    @Body() body: Record<string, unknown>,
  ): Promise<{ jobId: string; status: string; itemsCreated: number; replay: boolean }> {
    const workspaceId = requireWorkspaceId(headers);
    if (typeof body['targetStageId'] === 'string') {
      requireUuid('targetStageId', body['targetStageId']);
    } else {
      throw new BadRequestException('targetStageId is required');
    }
    if (typeof body['idempotencyKey'] !== 'string' || body['idempotencyKey'].trim() === '') {
      throw new BadRequestException('idempotencyKey is required');
    }

    const result = await this.service.submit(workspaceId, body);
    return {
      jobId: result.job.id,
      status: result.job.status,
      itemsCreated: result.itemsCreated,
      replay: !result.created,
    };
  }

  @Get(':id/failures')
  async failures(
    @Headers() headers: Headers_,
    @Param('id') id: string,
    @Query('limit') limit?: string,
  ) {
    return this.service.failures(requireWorkspaceId(headers), requireUuid('id', id), limit);
  }

  @Get(':id/batches')
  async batches(@Headers() headers: Headers_, @Param('id') id: string) {
    return this.service.batches(requireWorkspaceId(headers), requireUuid('id', id));
  }

  @Get(':id/transitions')
  transitions(
    @Headers() headers: Headers_,
    @Param('id') id: string,
    @Query('limit') limit?: string,
    @Query('cursor') cursor?: string,
  ) {
    return this.service.transitions(requireWorkspaceId(headers), requireUuid('id', id), {
      limit,
      cursor,
    });
  }

  @Get(':id')
  status(@Headers() headers: Headers_, @Param('id') id: string) {
    return this.service.status(requireWorkspaceId(headers), requireUuid('id', id));
  }
}
