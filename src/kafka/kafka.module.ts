import { Module } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { KafkaController } from './kafka.controller';
import { ConsumerService } from './consumer.service';

@Module({
  controllers: [KafkaController],
  providers: [ProducerService, ConsumerService],
  exports: [ProducerService, ConsumerService]
})
export class KafkaModule { }
