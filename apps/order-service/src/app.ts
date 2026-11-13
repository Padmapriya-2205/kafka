import express, { type ErrorRequestHandler, type Request, type Response } from "express";
import type { Logger } from "pino";
import type { Producer } from "kafkajs";
import { publishOrderCreated } from "../../../packages/shared/src/kafka.js";
import { createOrderCreatedEvent, ORDERS_TOPIC, type CreateOrderInput, type OrderItem } from "../../../packages/shared/src/order-event.js";

function parseOrderInput(body: unknown): CreateOrderInput | null {
  if (typeof body !== "object" || body === null) return null;
  const input = body as Record<string, unknown>;
  if (typeof input.userId !== "string" || input.userId.trim() === "") return null;
  if (!Array.isArray(input.items) || input.items.length === 0) return null;
  if (typeof input.totalAmount !== "number" || !Number.isFinite(input.totalAmount) || input.totalAmount < 0) return null;

  const items: OrderItem[] = [];
  for (const candidate of input.items) {
    if (typeof candidate !== "object" || candidate === null) return null;
    const item = candidate as Record<string, unknown>;
    if (typeof item.productId !== "string" || item.productId.trim() === "") return null;
    if (!Number.isInteger(item.quantity) || Number(item.quantity) <= 0) return null;
    items.push({ productId: item.productId, quantity: Number(item.quantity) });
  }

  return { userId: input.userId, items, totalAmount: input.totalAmount };
}

export function createOrderApp(
  producer: Pick<Producer, "send">,
  logger: Logger
): express.Express {
  const app = express();
  app.use(express.json({ limit: "1mb" }));

  app.get("/health", (_request: Request, response: Response) => {
    response.json({ status: "ok", kafka: "connected" });
  });

  app.post("/orders", async (request: Request, response: Response) => {
    const input = parseOrderInput(request.body);
    if (!input) {
      logger.warn({ route: "POST /orders" }, "Rejected invalid order request");
      response.status(400).json({
        error: "Request must include userId, non-empty items, and a non-negative totalAmount"
      });
      return;
    }

    const event = createOrderCreatedEvent(input);
    logger.info({
      route: "POST /orders",
      userId: input.userId,
      orderId: event.orderId,
      eventId: event.eventId,
      topic: ORDERS_TOPIC
    }, "Accepted order request");
    await publishOrderCreated(producer, event, logger);
    logger.info({ orderId: event.orderId, eventId: event.eventId }, "Returning published order response");
    response.status(201).json({ orderId: event.orderId, status: "published" });
  });

  const errorHandler: ErrorRequestHandler = (error, _request, response, _next) => {
    logger.error({ err: error }, "Order request failed");
    if (response.headersSent) return;
    const status = typeof error === "object" && error !== null && "type" in error
      && error.type === "entity.parse.failed" ? 400 : 503;
    response.status(status).json({
      error: status === 400 ? "Request body must be valid JSON" : "Order could not be published"
    });
  };
  app.use(errorHandler);

  return app;
}
