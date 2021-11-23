import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { HttpModule } from '@nestjs/common';

@Module({
  imports: [
        HttpModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
