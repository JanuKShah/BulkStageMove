import { BadRequestException, Body, Controller, Get, Headers, Param, Post } from '@nestjs/common';
import { requireUuid, requireWorkspaceId } from '../../shared/tenancy/workspace-guard';
import { Stage } from './stage.repository';
import { StageService } from './stage.service';

type Headers_ = Record<string, string | string[] | undefined>;

@Controller('stages')
export class StageController {
  constructor(private readonly service: StageService) {}

  @Get()
  list(@Headers() headers: Headers_): Promise<Stage[]> {
    return this.service.list(requireWorkspaceId(headers));
  }

  @Get(':id')
  getById(@Headers() headers: Headers_, @Param('id') id: string): Promise<Stage> {
    return this.service.getById(requireWorkspaceId(headers), requireUuid('id', id));
  }

  @Post('can-move')
  canMove(
    @Headers() headers: Headers_,
    @Body() body: { from?: string; to?: string },
  ): Promise<{ allowed: boolean }> {
    const { from, to } = body;
    if (!from || !to) throw new BadRequestException('from and to are required');
    return this.service
      .isMoveAllowed(requireWorkspaceId(headers), requireUuid('from', from), requireUuid('to', to))
      .then((allowed) => ({ allowed }));
  }

  @Post('allowed-targets')
  async allowedTargets(
    @Headers() headers: Headers_,
    @Body() body: { from?: string },
  ): Promise<{ to: string[] }> {
    if (!body.from) throw new BadRequestException('from is required');
    return {
      to: await this.service.allowedTargets(
        requireWorkspaceId(headers),
        requireUuid('from', body.from),
      ),
    };
  }
}
