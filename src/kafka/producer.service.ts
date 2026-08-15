import { Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { Admin, Kafka, Producer, ProducerRecord } from 'kafkajs';

@Injectable()
export class ProducerService implements OnModuleInit, OnApplicationShutdown {
  private readonly kafka = new Kafka({
    clientId: 'nestjs-kafka',
    brokers: ['localhost:9092'],
  });
  private readonly admin: Admin = this.kafka.admin();
  private readonly producer: Producer = this.kafka.producer();

  async onModuleInit() {
    await this.admin.connect();
    await this.ensureTopicExists('test');
    await this.producer.connect();
  }

  async ensureTopicExists(topic: string) {
    try {
      await this.admin.createTopics({
        waitForLeaders: true,
        topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
      });
    } catch (error) {
      const kafkaError = error as { type?: string };
      if (kafkaError.type !== 'TOPIC_ALREADY_EXISTS') {
        throw error;
      }
    }
  }

  async produce(record: ProducerRecord) {
    await this.ensureTopicExists(record.topic);
    await this.producer.send(record);
  }

  async onApplicationShutdown() {
    await this.producer.disconnect();
    await this.admin.disconnect();
  }
}
