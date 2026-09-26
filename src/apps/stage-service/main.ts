import { NestFactory } from '@nestjs/core';
import { StageModule } from './stage.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(StageModule);
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT ?? 3003), '0.0.0.0');
}

void bootstrap();
