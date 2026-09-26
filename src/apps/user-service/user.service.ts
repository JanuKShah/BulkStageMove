import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from '@nestjs/common';
import { isUniqueViolation } from '../../shared/database/unique-violation';
import { User, UserRepository } from './user.repository';

const MAX_LIMIT = 500;
const DEFAULT_LIMIT = 100;

const EMAIL_UNIQUE = 'app_user_workspace_email_uniq';

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
    try {
      return await this.repository.create(workspaceId, trimmed, email ?? null);
    } catch (error) {
      // The constraint is the authority, not a pre-check: two concurrent creates
      // with the same address can both pass a lookup, and only one can win.
      if (isUniqueViolation(error, EMAIL_UNIQUE)) {
        throw new ConflictException(`email ${email} is already in use in this workspace`);
      }
      throw error;
    }
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
