import { Injectable, OnApplicationShutdown } from '@nestjs/common';
import { Admin, Consumer, ConsumerRunConfig, ConsumerSubscribeTopics, Kafka } from 'kafkajs';

@Injectable()
export class ConsumerService implements OnApplicationShutdown {
  private readonly kafka = new Kafka({
    clientId: 'nestjs-kafka',
    brokers: ['localhost:9092'],
  });
  private readonly admin: Admin = this.kafka.admin();
  private readonly consumers: Consumer[] = [];

  async ensureTopicExists(topic: string) {
    try {
      await this.admin.connect();
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

  async consume(topic: ConsumerSubscribeTopics, config: ConsumerRunConfig) {
    for (const topicName of topic.topics) {
      await this.ensureTopicExists(topicName.toString());
    }

    const consumer = this.kafka.consumer({ groupId: 'nestjs-kafka' });
    await consumer.connect();
    await consumer.subscribe(topic);
    await consumer.run(config);
    this.consumers.push(consumer);
  }

  async onApplicationShutdown() {
    for (const consumer of this.consumers) {
      await consumer.disconnect();
    }
    await this.admin.disconnect();
  }
}