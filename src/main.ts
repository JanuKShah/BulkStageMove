import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);

  // Lets the container stop cleanly on SIGTERM instead of being killed mid-query.
  app.enableShutdownHooks();

  const port = Number(process.env.PORT ?? 3000);
  // 0.0.0.0, not localhost - the latter is unreachable from outside the container.
  await app.listen(port, '0.0.0.0');
}

void bootstrap();
