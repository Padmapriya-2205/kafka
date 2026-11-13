import "dotenv/config";
import { createKafka, ensureOrdersTopic } from "../../../packages/shared/src/kafka.js";
import { createLogger } from "../../../packages/shared/src/logger.js";
import { createOrderApp } from "./app.js";

const logger = createLogger("order-service");

async function main(): Promise<void> {
  const port = Number(process.env.ORDER_SERVICE_PORT ?? "3000");
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    throw new Error("ORDER_SERVICE_PORT must be a valid TCP port");
  }

  const kafka = createKafka("order-service");
  logger.info({ brokers: process.env.KAFKA_BROKERS ?? "localhost:9092" }, "Connecting to Kafka");
  await ensureOrdersTopic(kafka, logger);
  const producer = kafka.producer();
  await producer.connect();
  logger.info("Kafka producer connected");

  const app = createOrderApp(producer, logger);

  const server = app.listen(port, () => {
    logger.info({ port }, "Order Service listening");
  });

  let stopping = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (stopping) return;
    stopping = true;
    logger.info({ signal }, "Shutting down Order Service");
    await new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
    await producer.disconnect();
    logger.info("Kafka producer disconnected");
  };

  process.once("SIGINT", () => { void shutdown("SIGINT").catch((error) => { console.error(error); process.exitCode = 1; }); });
  process.once("SIGTERM", () => { void shutdown("SIGTERM").catch((error) => { console.error(error); process.exitCode = 1; }); });
}

main().catch((error: unknown) => {
  logger.fatal({ err: error }, "Order Service failed to start");
  process.exitCode = 1;
});
