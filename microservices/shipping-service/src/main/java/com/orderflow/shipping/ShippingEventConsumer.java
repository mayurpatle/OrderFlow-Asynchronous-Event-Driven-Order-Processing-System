package com.orderflow.shipping;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.orderflow.common.events.EventEnvelope;
import com.orderflow.common.events.Topics;
import com.orderflow.common.events.payloads.PaymentCompletedPayload;
import com.orderflow.common.events.payloads.ShippingDispatchedPayload;
import com.orderflow.shipping.Entity.Shipment;
import com.orderflow.shipping.Entity.ShipmentStatus;
import com.orderflow.shipping.Repository.ShipmentRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * The shipping-service's Kafka handler.
 *
 * Pattern: consume payment.completed → create a shipment → publish shipping.dispatched.
 *
 * Single-path logic — shipping always succeeds in our simulation. We don't
 * model carrier failures, address validation, or capacity issues. Real
 * shipping operations have all these concerns; we defer them to keep this
 * session focused on cementing the consume-decide-produce pattern.
 *
 * Why we consume payment.completed and not inventory.reserved:
 *   We ship only AFTER payment is confirmed. If we shipped on inventory.reserved,
 *   we'd be sending packages for orders the customer hadn't paid for. The
 *   ordering — pay first, ship second — is enforced through which topic
 *   we subscribe to. Same architectural principle as why payment-service
 *   consumes inventory.reserved instead of order.created.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class ShippingEventConsumer {

    private final ShipmentRepository shipmentRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = Topics.PAYMENT_COMPLETED,
            groupId = "shipping-service",
            containerFactory = "orderflowKafkaListenerFactory"
    )
    @Transactional
    public void onPaymentCompleted(EventEnvelope<Map<String, Object>> envelope) {
        PaymentCompletedPayload paid = objectMapper.convertValue(
                envelope.payload(), PaymentCompletedPayload.class);

        log.info("Processing payment.completed for orderId={} amountCents={}",
                paid.orderId(), paid.amountCents());

        // Idempotency check: did we already ship this order?
        // If yes, this is a redelivery from Kafka's at-least-once semantics.
        // Skip silently — the first dispatch is the canonical one.
        if (shipmentRepo.findByOrderId(paid.orderId()).isPresent()) {
            log.info("Shipment already exists for order {}, skipping (idempotent)",
                    paid.orderId());
            return;
        }

        // Create the shipment. In production this would call the carrier's API
        // (FedEx, UPS, Bluedart, Shiprocket) to register the package and get
        // a real tracking number. We simulate with a generated reference.
        Shipment shipment = Shipment.builder()
                .id(UUID.randomUUID())
                .orderId(paid.orderId())
                .carrier("FEDEX")
                .trackingNumber("FX" + System.currentTimeMillis())
                .status(ShipmentStatus.DISPATCHED)
                .createdAt(Instant.now())
                .estimatedDeliveryAt(Instant.now().plus(Duration.ofDays(3)))
                .build();
        shipmentRepo.save(shipment);

        // Publish the success event.
        // Same correlationId propagation pattern: continue the trace.
        publishShippingDispatched(shipment, envelope);
    }

    private void publishShippingDispatched(
            Shipment shipment,
            EventEnvelope<Map<String, Object>> sourceEnvelope) {

        ShippingDispatchedPayload payload = new ShippingDispatchedPayload(
                shipment.getOrderId(),
                shipment.getId(),
                shipment.getCarrier(),
                shipment.getTrackingNumber(),
                shipment.getEstimatedDeliveryAt()
        );

        EventEnvelope<ShippingDispatchedPayload> envelope = EventEnvelope.of(
                "shipping.dispatched",
                payload,
                sourceEnvelope.correlationId(),
                sourceEnvelope.eventId()
        );

        kafkaTemplate.send(Topics.SHIPPING_DISPATCHED, shipment.getOrderId().toString(), envelope);

        log.info("Dispatched shipment {} for order {} via {} (tracking: {})",
                shipment.getId(),
                shipment.getOrderId(),
                shipment.getCarrier(),
                shipment.getTrackingNumber());
    }
}