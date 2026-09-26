import { NestFactory } from '@nestjs/core';
import { OpportunityModule } from './opportunity.module';

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(OpportunityModule);
  app.enableShutdownHooks();
  await app.listen(Number(process.env.PORT ?? 3004), '0.0.0.0');
}

void bootstrap();
