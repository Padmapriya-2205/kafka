import { Kafka, type Consumer, type EachMessagePayload, type KafkaConfig, type Producer, type TopicPartitionOffset } from "kafkajs";
import type { Logger } from "pino";
import {
  ORDERS_TOPIC,
  parseOrderCreatedEvent,
  type OrderCreatedEvent
} from "./order-event.js";

export function createKafka(clientId: string): Kafka {
  const brokers = (process.env.KAFKA_BROKERS ?? "localhost:9092")
    .split(",")
    .map((broker) => broker.trim())
    .filter(Boolean);

  if (brokers.length === 0) {
    throw new Error("KAFKA_BROKERS must contain at least one broker");
  }

  const config: KafkaConfig = {
    clientId,
    brokers,
    retry: { initialRetryTime: 300, retries: 8 }
  };
  return new Kafka(config);
}

export async function ensureOrdersTopic(kafka: Kafka, logger: Logger): Promise<void> {
  const admin = kafka.admin();
  await admin.connect();
  try {
    const topics = await admin.listTopics();
    if (topics.includes(ORDERS_TOPIC)) {
      logger.info({
        topic: ORDERS_TOPIC,
        partitions: 3,
        replicationFactor: 1,
        created: false
      }, "Kafka topic already exists");
      return;
    }

    const created = await admin.createTopics({
      topics: [{ topic: ORDERS_TOPIC, numPartitions: 3, replicationFactor: 1 }],
      waitForLeaders: true
    });
    logger.info({
      topic: ORDERS_TOPIC,
      partitions: 3,
      replicationFactor: 1,
      created
    }, created ? "Kafka topic created" : "Kafka topic already exists");
  } finally {
    await admin.disconnect();
  }
}

export async function publishOrderCreated(
  producer: Pick<Producer, "send">,
  event: OrderCreatedEvent,
  logger: Logger
): Promise<void> {
  logger.info({
    topic: ORDERS_TOPIC,
    eventId: event.eventId,
    eventType: event.eventType,
    orderId: event.orderId,
    userId: event.userId,
    itemCount: event.items.length,
    messageKey: event.orderId
  }, "Publishing order event");

  try {
    const metadata = await producer.send({
      topic: ORDERS_TOPIC,
      acks: -1,
      messages: [{ key: event.orderId, value: JSON.stringify(event) }]
    });
    logger.info({
      topic: ORDERS_TOPIC,
      eventId: event.eventId,
      orderId: event.orderId,
      partitions: metadata.map(({ partition, baseOffset }) => ({ partition, baseOffset }))
    }, "Order event published");
  } catch (error) {
    logger.error({ err: error, topic: ORDERS_TOPIC, eventId: event.eventId, orderId: event.orderId }, "Order event publish failed");
    throw error;
  }
}

export async function processOrderMessage(
  payload: Pick<EachMessagePayload, "topic" | "partition" | "message">,
  commitOffsets: (offsets: TopicPartitionOffset[]) => Promise<void>,
  handle: (event: OrderCreatedEvent) => Promise<void>,
  logger: Logger
): Promise<void> {
  if (payload.topic !== ORDERS_TOPIC || payload.message.value === null) {
    throw new Error("Received an empty message or a message from an unexpected topic");
  }

  const value = JSON.parse(payload.message.value.toString("utf8")) as unknown;
  const event = parseOrderCreatedEvent(value);
  logger.info({
    topic: payload.topic,
    partition: payload.partition,
    offset: payload.message.offset,
    eventId: event.eventId,
    eventType: event.eventType,
    orderId: event.orderId,
    userId: event.userId
  }, "Order event received");

  await handle(event);

  // Kafka stores the next offset to read, so commit the processed offset + 1.
  const nextOffset = (BigInt(payload.message.offset) + 1n).toString();
  await commitOffsets([{
    topic: payload.topic,
    partition: payload.partition,
    offset: nextOffset
  }]);
  logger.info({
    topic: payload.topic,
    partition: payload.partition,
    processedOffset: payload.message.offset,
    committedOffset: nextOffset,
    eventId: event.eventId,
    orderId: event.orderId
  }, "Order event processed and offset committed");
}

export async function connectProducer(producer: Producer): Promise<void> {
  await producer.connect();
}

export async function createConnectedConsumer(
  kafka: Kafka,
  groupId: string,
  logger: Logger,
  fromBeginning = true
): Promise<Consumer> {
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  logger.info({ topic: ORDERS_TOPIC, groupId, fromBeginning }, "Kafka consumer connected");
  try {
    await consumer.subscribe({ topic: ORDERS_TOPIC, fromBeginning });
    logger.info({ topic: ORDERS_TOPIC, groupId }, "Kafka consumer subscribed");
    return consumer;
  } catch (error) {
    logger.error({ err: error, topic: ORDERS_TOPIC, groupId }, "Kafka consumer subscription failed");
    await consumer.disconnect();
    throw error;
  }
}

export function makeEachMessageHandler(
  consumer: Consumer,
  logger: Logger,
  handle: (event: OrderCreatedEvent) => Promise<void>
): (payload: EachMessagePayload) => Promise<void> {
  return async (payload) => {
    try {
      await processOrderMessage(payload, (offsets) => consumer.commitOffsets(offsets), handle, logger);
    } catch (error) {
      logger.error({
        err: error,
        topic: payload.topic,
        partition: payload.partition,
        offset: payload.message.offset,
        key: payload.message.key?.toString("utf8")
      }, "Failed to process order event; offset was not committed");
      throw error;
    }
  };
}
