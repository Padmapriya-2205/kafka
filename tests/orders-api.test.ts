import { beforeEach, describe, expect, it, jest } from "@jest/globals";
import type { Express } from "express";
import pino from "pino";
import type { Producer } from "kafkajs";
import request from "supertest";
import { createOrderApp } from "../apps/order-service/src/app.js";
import { ORDERS_TOPIC, type OrderCreatedEvent } from "../packages/shared/src/order-event.js";

describe("POST /orders", () => {
  let app: Express;
  let send: jest.Mock<Producer["send"]>;

  beforeEach(() => {
    send = jest.fn<Producer["send"]>().mockResolvedValue([]);
    app = createOrderApp({ send }, pino({ level: "silent" }));
  });

  it("publishes a valid order and returns its generated order ID", async () => {
    const response = await request(app)
      .post("/orders")
      .send({
        userId: "user-101",
        items: [{ productId: "product-101", quantity: 2 }],
        totalAmount: 1500
      });

    expect(response.status).toBe(201);
    expect(response.body).toEqual({ orderId: expect.any(String), status: "published" });
    expect(send).toHaveBeenCalledTimes(1);

    const [publishRequest] = send.mock.calls[0];
    expect(publishRequest.topic).toBe(ORDERS_TOPIC);
    expect(publishRequest.messages).toHaveLength(1);
    expect(publishRequest.messages[0].key).toBe(response.body.orderId);

    const event = JSON.parse(publishRequest.messages[0].value as string) as OrderCreatedEvent;
    expect(event).toMatchObject({
      eventType: "ORDER_CREATED",
      orderId: response.body.orderId,
      userId: "user-101",
      items: [{ productId: "product-101", quantity: 2 }],
      totalAmount: 1500
    });
    expect(event.eventId).toEqual(expect.any(String));
    expect(Date.parse(event.createdAt)).not.toBeNaN();
  });

  it("rejects invalid order input without publishing", async () => {
    const response = await request(app)
      .post("/orders")
      .send({ userId: "user-101", items: [], totalAmount: 1500 });

    expect(response.status).toBe(400);
    expect(response.body.error).toMatch(/userId/);
    expect(send).not.toHaveBeenCalled();
  });

  it("returns 503 when Kafka publishing fails", async () => {
    send.mockRejectedValue(new Error("broker unavailable"));

    const response = await request(app)
      .post("/orders")
      .send({
        userId: "user-101",
        items: [{ productId: "product-101", quantity: 2 }],
        totalAmount: 1500
      });

    expect(response.status).toBe(503);
    expect(response.body).toEqual({ error: "Order could not be published" });
  });
});
