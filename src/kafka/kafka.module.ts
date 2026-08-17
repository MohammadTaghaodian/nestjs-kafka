import { Module } from '@nestjs/common';
import { ProducerService } from './producer.service';
import { ConsumerService } from './consumer.service';

@Module({
  controllers: [],
  providers: [ProducerService, ConsumerService],
  exports: [ProducerService, ConsumerService]
})
export class KafkaModule { }
