package com.orderflow.common.events;
/**
 * Centralized topic name constants.
 *
 * Why a class instead of strings scattered everywhere?
 *
 *   - A typo in a topic name is a silent bug. The producer publishes to
 *     "order.created" but the consumer listens on "orders.created" (typo).
 *     With auto-create-topics enabled it just works — events go nowhere.
 *     Constants make typos compiler errors instead.
 *
 *   - When we rename a topic, IDE refactoring updates every reference at once.
 *
 *   - Code review becomes easier — you can grep for "Topics.ORDER_CREATED"
 *     and find every producer and consumer of the topic.
 *
 * Naming convention:
 *
 *   <noun>.<past-tense-verb>     e.g. order.created, payment.completed
 *
 *   We use the past tense because events are FACTS that already happened.
 *   "order.created" means "an order was created." If you ever find yourself
 *   tempted to write "order.create" or "create.order" — those are commands,
 *   not events. Events are always past tense. This is a hard rule.
 */
public final  class Topics {
    // Private constructor — this class is a namespace, not instantiable.
    private Topics() {}

    // --- Order service events ---
    public static final String ORDER_CREATED   = "order.created";
    public static final String ORDER_CANCELLED = "order.cancelled";

    // --- Inventory service events ---
    public static final String INVENTORY_RESERVED            = "inventory.reserved";
    public static final String INVENTORY_RESERVATION_FAILED  = "inventory.reservation_failed";

    // --- Payment service events ---
    public static final String PAYMENT_COMPLETED = "payment.completed";
    public static final String PAYMENT_FAILED    = "payment.failed";

    // --- Shipping service events ---
    public static final String SHIPPING_DISPATCHED = "shipping.dispatched";
    public static final String SHIPPING_DELIVERED = "shipping.delivered";

    // --- Notification service events ---
    public static final String NOTIFICATION_SENT = "notification.sent";



}
