import "dotenv/config";
import { createKafka, createConnectedConsumer, ensureOrdersTopic, makeEachMessageHandler } from "../../../packages/shared/src/kafka.js";
import { createLogger } from "../../../packages/shared/src/logger.js";
import { INVENTORY_GROUP, type OrderCreatedEvent } from "../../../packages/shared/src/order-event.js";

const logger = createLogger("inventory-service").child({ consumerGroup: INVENTORY_GROUP });

async function reserveInventory(order: OrderCreatedEvent): Promise<void> {
  logger.info({ orderId: order.orderId, itemCount: order.items.length }, "Checking inventory");
  const items = order.items.map(({ productId, quantity }) => ({ productId, quantity }));
  logger.info({ orderId: order.orderId, items, status: "reserved" }, "Inventory reserved");
}

async function main(): Promise<void> {
  const kafka = createKafka("inventory-service");
  logger.info({ brokers: process.env.KAFKA_BROKERS ?? "localhost:9092" }, "Connecting to Kafka");
  await ensureOrdersTopic(kafka, logger);
  const consumer = await createConnectedConsumer(kafka, INVENTORY_GROUP, logger);
  await new Promise<void>((resolve) => {
    let stopping = false;

    const shutdown = async (signal: string): Promise<void> => {
      if (stopping) return;
      stopping = true;
      logger.info({ signal }, "Stopping Inventory Service consumer");
      try {
        await consumer.stop();
        await consumer.disconnect();
        logger.info("Inventory Service consumer disconnected");
      } catch (error) {
        logger.error({ err: error }, "Failed to cleanly stop Inventory Service consumer");
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
      eachMessage: makeEachMessageHandler(consumer, logger, reserveInventory)
    }).catch((error: unknown) => {
      logger.fatal({ err: error }, "Inventory Service consumer failed to start");
      process.exitCode = 1;
      void shutdown("consumer-run-failed");
    });
    logger.info("Inventory Service consuming order events");
  });
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, "Inventory Service stopped unexpectedly");
  process.exitCode = 1;
});
