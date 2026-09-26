import { NestFactory } from '@nestjs/core';
import { UserModule } from './user.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(UserModule);
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT ?? 3002), '0.0.0.0');
}

void bootstrap();
