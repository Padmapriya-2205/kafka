import { describe, expect, it } from "@jest/globals";
import { createOrderCreatedEvent, parseOrderCreatedEvent } from "../packages/shared/src/order-event.js";

describe("ORDER_CREATED events", () => {
  it("creates uniquely identified ORDER_CREATED events", () => {
    const input = {
      userId: "user-101",
      items: [{ productId: "product-101", quantity: 2 }],
      totalAmount: 1500
    };
    const first = createOrderCreatedEvent(input);
    const second = createOrderCreatedEvent(input);

    expect(first.eventType).toBe("ORDER_CREATED");
    expect(first.userId).toBe(input.userId);
    expect(first.orderId).not.toBe(second.orderId);
    expect(first.eventId).not.toBe(second.eventId);
    expect(Date.parse(first.createdAt)).not.toBeNaN();
    expect(parseOrderCreatedEvent(first)).toEqual(first);
  });

  it("rejects malformed order events", () => {
    expect(() => parseOrderCreatedEvent({ eventType: "ORDER_CREATED" })).toThrow("not a valid");
  });
});
