import { describe, expect, it } from "@jest/globals";
import pino from "pino";
import { processOrderMessage, publishOrderCreated } from "../packages/shared/src/kafka.js";
import { INVENTORY_GROUP, NOTIFICATION_GROUP, ORDERS_TOPIC, type OrderCreatedEvent } from "../packages/shared/src/order-event.js";
import type { EachMessagePayload, Producer, TopicPartitionOffset } from "kafkajs";

const event: OrderCreatedEvent = {
  eventId: "event-101",
  eventType: "ORDER_CREATED",
  orderId: "order-101",
  userId: "user-101",
  items: [{ productId: "product-101", quantity: 2 }],
  totalAmount: 1500,
  createdAt: "2026-01-01T00:00:00.000Z"
};
const testLogger = pino({ level: "silent" });

describe("Kafka order processing", () => {
  it("publishes an order with orderId as the Kafka message key", async () => {
    let sent: Parameters<Producer["send"]>[0] | undefined;
    const producer = {
      send: async (message: Parameters<Producer["send"]>[0]) => {
        sent = message;
        return [];
      }
    };

    await publishOrderCreated(producer, event, testLogger);
    expect(sent).toEqual({
      topic: ORDERS_TOPIC,
      acks: -1,
      messages: [{ key: "order-101", value: JSON.stringify(event) }]
    });
  });

  it("calls the handler before committing the next offset", async () => {
    const order: string[] = [];
    let committed: TopicPartitionOffset[] | undefined;
    const payload = {
      topic: ORDERS_TOPIC,
      partition: 2,
      message: { value: Buffer.from(JSON.stringify(event)), offset: "8" }
    } as EachMessagePayload;

    await processOrderMessage(
      payload,
      async (offsets) => {
        order.push("commit");
        committed = offsets;
      },
      async (received) => {
        expect(received).toEqual(event);
        order.push("handle");
      },
      testLogger
    );

    expect(order).toEqual(["handle", "commit"]);
    expect(committed).toEqual([{ topic: ORDERS_TOPIC, partition: 2, offset: "9" }]);
  });

  it("does not commit when order processing fails", async () => {
    let committed = false;
    const payload = {
      topic: ORDERS_TOPIC,
      partition: 0,
      message: { value: Buffer.from(JSON.stringify(event)), offset: "0" }
    } as EachMessagePayload;

    await expect(processOrderMessage(
      payload,
      async () => { committed = true; },
      async () => { throw new Error("inventory unavailable"); },
      testLogger
    )).rejects.toThrow("inventory unavailable");
    expect(committed).toBe(false);
  });

  it("delivers an event independently to inventory and notification groups", async () => {
    const processedBy: string[] = [];
    const committedOffsets = new Map<string, TopicPartitionOffset[]>();
    const payload = {
      topic: ORDERS_TOPIC,
      partition: 1,
      message: { value: Buffer.from(JSON.stringify(event)), offset: "12" }
    } as EachMessagePayload;

    for (const groupId of [INVENTORY_GROUP, NOTIFICATION_GROUP]) {
      await processOrderMessage(
        payload,
        async (offsets) => { committedOffsets.set(groupId, offsets); },
        async (received) => {
          expect(received).toEqual(event);
          processedBy.push(groupId);
        },
        testLogger
      );
    }

    expect(INVENTORY_GROUP).not.toBe(NOTIFICATION_GROUP);
    expect(processedBy).toEqual([INVENTORY_GROUP, NOTIFICATION_GROUP]);
    expect(committedOffsets.get(INVENTORY_GROUP)).toEqual([
      { topic: ORDERS_TOPIC, partition: 1, offset: "13" }
    ]);
    expect(committedOffsets.get(NOTIFICATION_GROUP)).toEqual([
      { topic: ORDERS_TOPIC, partition: 1, offset: "13" }
    ]);
  });
});
