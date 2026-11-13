import "dotenv/config";
import { createKafka, createConnectedConsumer, ensureOrdersTopic, makeEachMessageHandler } from "../../../packages/shared/src/kafka.js";
import { createLogger } from "../../../packages/shared/src/logger.js";
import { NOTIFICATION_GROUP, type OrderCreatedEvent } from "../../../packages/shared/src/order-event.js";

const logger = createLogger("notification-service").child({ consumerGroup: NOTIFICATION_GROUP });

async function sendConfirmation(order: OrderCreatedEvent): Promise<void> {
  logger.info({
    orderId: order.orderId,
    userId: order.userId,
    status: "sent"
  }, "Order confirmation notification sent");
}

async function main(): Promise<void> {
  const kafka = createKafka("notification-service");
  logger.info({ brokers: process.env.KAFKA_BROKERS ?? "localhost:9092" }, "Connecting to Kafka");
  await ensureOrdersTopic(kafka, logger);
  const consumer = await createConnectedConsumer(kafka, NOTIFICATION_GROUP, logger);
  await new Promise<void>((resolve) => {
    let stopping = false;

    const shutdown = async (signal: string): Promise<void> => {
      if (stopping) return;
      stopping = true;
      logger.info({ signal }, "Stopping Notification Service consumer");
      try {
        await consumer.stop();
        await consumer.disconnect();
        logger.info("Notification Service consumer disconnected");
      } catch (error) {
        logger.error({ err: error }, "Failed to cleanly stop Notification Service consumer");
        process.exitCode = 1;
      } finally {
        resolve();
      }
    };

    process.once("SIGINT", () => { void shutdown("SIGINT"); });
    process.once("SIGTERM", () => { void shutdown("SIGTERM"); });
    consumer.on(consumer.events.CRASH, ({ payload }) => {
      logger.error({
        err: payload.error,
        consumerGroup: payload.groupId,
        willRestart: payload.restart
      }, "Kafka consumer crashed");
    });

    void consumer.run({
      autoCommit: false,
      eachMessage: makeEachMessageHandler(consumer, logger, sendConfirmation)
    }).catch((error: unknown) => {
      logger.fatal({ err: error }, "Notification Service consumer failed to start");
      process.exitCode = 1;
      void shutdown("consumer-run-failed");
    });
    logger.info("Notification Service consuming order events");
  });
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, "Notification Service stopped unexpectedly");
  process.exitCode = 1;
});
