import { NestFactory } from '@nestjs/core';
import { WorkspaceModule } from './workspace.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(WorkspaceModule);
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT ?? 3001), '0.0.0.0');
}

void bootstrap();
