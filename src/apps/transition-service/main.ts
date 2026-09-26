import { NestFactory } from '@nestjs/core';
import { TransitionModule } from './transition.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(TransitionModule);
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT ?? 3005), '0.0.0.0');
}

void bootstrap();
