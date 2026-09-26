import {
  BadRequestException,
  Body,
  Controller,
  Get,
  Headers,
  HttpCode,
  Param,
  Post,
} from '@nestjs/common';
import { requireUuid, requireWorkspaceId } from '../../shared/tenancy/workspace-guard';
import { Stage } from './stage.repository';
import { StageService } from './stage.service';

type Headers_ = Record<string, string | string[] | undefined>;

/**
 * Two read-only predicates, deliberately exposed as POST rather than GET.
 *
 * A predicate over two UUIDs has no natural resource URI, and a query string
 * carrying two UUIDs is worse to read, log and cache than a small JSON body.
 * That is a deliberate choice about ergonomics, not an oversight.
 *
 * The status is still 200: neither call creates nor modifies anything. NestJS
 * defaults every POST to 201, which would be a lie here.
 */
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
  @HttpCode(200)
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
  @HttpCode(200)
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
