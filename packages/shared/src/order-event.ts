import { randomUUID } from "node:crypto";

export const ORDERS_TOPIC = "orders-created";
export const INVENTORY_GROUP = "inventory-group";
export const NOTIFICATION_GROUP = "notification-group";

export interface OrderItem {
  productId: string;
  quantity: number;
}

export interface OrderCreatedEvent {
  eventId: string;
  eventType: "ORDER_CREATED";
  orderId: string;
  userId: string;
  items: OrderItem[];
  totalAmount: number;
  createdAt: string;
}

export interface CreateOrderInput {
  userId: string;
  items: OrderItem[];
  totalAmount: number;
}

export function createOrderCreatedEvent(input: CreateOrderInput): OrderCreatedEvent {
  return {
    eventId: randomUUID(),
    eventType: "ORDER_CREATED",
    orderId: randomUUID(),
    userId: input.userId,
    items: input.items,
    totalAmount: input.totalAmount,
    createdAt: new Date().toISOString()
  };
}

export function parseOrderCreatedEvent(value: unknown): OrderCreatedEvent {
  if (!isOrderCreatedEvent(value)) {
    throw new Error("Message is not a valid ORDER_CREATED event");
  }

  return value;
}

function isOrderCreatedEvent(value: unknown): value is OrderCreatedEvent {
  if (typeof value !== "object" || value === null) return false;
  const event = value as Record<string, unknown>;

  return event.eventType === "ORDER_CREATED"
    && typeof event.eventId === "string"
    && event.eventId.length > 0
    && typeof event.orderId === "string"
    && event.orderId.length > 0
    && typeof event.userId === "string"
    && event.userId.length > 0
    && Array.isArray(event.items)
    && event.items.every(isOrderItem)
    && typeof event.totalAmount === "number"
    && Number.isFinite(event.totalAmount)
    && event.totalAmount >= 0
    && typeof event.createdAt === "string"
    && !Number.isNaN(Date.parse(event.createdAt));
}

function isOrderItem(value: unknown): value is OrderItem {
  if (typeof value !== "object" || value === null) return false;
  const item = value as Record<string, unknown>;
  return typeof item.productId === "string"
    && item.productId.length > 0
    && Number.isInteger(item.quantity)
    && Number(item.quantity) > 0;
}
