import { BadRequestException, Injectable, NotFoundException } from '@nestjs/common';
import { User, UserRepository } from './user.repository';

const MAX_LIMIT = 500;
const DEFAULT_LIMIT = 100;

@Injectable()
export class UserService {
  constructor(private readonly repository: UserRepository) {}

  async create(
    workspaceId: string,
    name: string | undefined,
    email: string | undefined,
  ): Promise<User> {
    const trimmed = name?.trim();
    if (!trimmed) throw new BadRequestException('name is required');
    return this.repository.create(workspaceId, trimmed, email ?? null);
  }

  async list(workspaceId: string, limit: string | undefined): Promise<User[]> {
    const parsed = Number(limit ?? DEFAULT_LIMIT);
    const take = Math.min(
      Number.isFinite(parsed) && parsed > 0 ? parsed : DEFAULT_LIMIT,
      MAX_LIMIT,
    );
    return this.repository.list(workspaceId, take);
  }

  async getById(workspaceId: string, id: string): Promise<User> {
    const found = await this.repository.findById(workspaceId, id);
    if (!found) throw new NotFoundException(`user ${id} not found`);
    return found;
  }
}
