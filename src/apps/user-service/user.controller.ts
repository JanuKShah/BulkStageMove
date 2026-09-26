import { Body, Controller, Get, Headers, Param, Post, Query } from '@nestjs/common';
import { requireUuid, requireWorkspaceId } from '../../shared/tenancy/workspace-guard';
import { User } from './user.repository';
import { UserService } from './user.service';

type Headers_ = Record<string, string | string[] | undefined>;

@Controller('users')
export class UserController {
  constructor(private readonly service: UserService) {}

  @Post()
  create(
    @Headers() headers: Headers_,
    @Body() body: { name?: string; email?: string },
  ): Promise<User> {
    return this.service.create(requireWorkspaceId(headers), body.name, body.email);
  }

  @Get()
  list(@Headers() headers: Headers_, @Query('limit') limit?: string): Promise<User[]> {
    return this.service.list(requireWorkspaceId(headers), limit);
  }

  @Get(':id')
  getById(@Headers() headers: Headers_, @Param('id') id: string): Promise<User> {
    return this.service.getById(requireWorkspaceId(headers), requireUuid('id', id));
  }
}
