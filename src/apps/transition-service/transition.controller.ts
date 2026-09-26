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
   * Always 201, including when the key replays an existing job.
   *
   * A replay is not a lesser success: the job exists and the client is holding
   * it, exactly as if this request had created it. Answering 200 on the retry
   * would make the contract only half idempotent - the body would be stable
   * while the status changed - and a client branching on the status would then
   * behave differently on the retry path than on the original.
   *
   * Whether this call created the job or found it is reported as `replay` in the
   * body, so the two facts are never carried by two channels that can disagree.
   * The different-key-same-as-another-request case is a genuine 409 and is
   * raised in the service.
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
