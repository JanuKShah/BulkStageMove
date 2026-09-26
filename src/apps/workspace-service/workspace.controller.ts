import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { requireUuid } from '../../shared/tenancy/workspace-guard';
import { Workspace } from './workspace.repository';
import { WorkspaceService } from './workspace.service';

@Controller('workspaces')
export class WorkspaceController {
  constructor(private readonly service: WorkspaceService) {}

  @Post()
  create(@Body() body: { name?: string }): Promise<Workspace> {
    return this.service.create(body.name);
  }

  @Get(':id')
  getById(@Param('id') id: string): Promise<Workspace> {
    return this.service.getById(requireUuid('id', id));
  }
}
