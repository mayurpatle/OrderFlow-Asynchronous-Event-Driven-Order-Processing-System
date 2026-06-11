package com.orderflow.payment.Entity;

import com.orderflow.payment.PaymentStatus;
import jakarta.persistence.*;
import lombok.*;

import java.time.Instant;
import java.util.UUID;

/**
 * A single payment attempt.
 *
 * Every call to the gateway — successful or failed — produces one row.
 * This is non-negotiable in real payment systems for audit, compliance,
 * and reconciliation purposes.
 *
 * Why orderId is unique:
 *   In our model, we attempt payment exactly once per order. If the first
 *   attempt fails, the order is cancelled (in Session 9's saga). We never
 *   retry the same order automatically — that decision belongs to the
 *   customer ("try another card"), which would create a new order.
 *
 *   Real systems sometimes allow multiple payment attempts per order. In
 *   that case orderId would NOT be unique, and you'd track attempts via
 *   a separate "attempt number" column. We keep it simple for now.
 */
@Entity
@Table(name = "payments")
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Payment {

    @Id
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    /**
     * Amount in the smallest currency unit. 5998 means $59.98 USD.
     * Long, not double — doubles have rounding errors that compound over
     * millions of transactions.
     */
    @Column(nullable = false)
    private long amountCents;

    @Column(nullable = false)
    private String currency;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private PaymentStatus status;

    /**
     * The gateway's transaction ID for successful payments.
     *
     * Nullable because failed payments may not have a reference — the
     * gateway may have rejected the request before creating a transaction.
     * For successful payments, this is the ID you'd use to issue a refund
     * or look up the charge in the gateway's dashboard.
     */
    @Column
    private String gatewayReference;

    /**
     * Failure code from the gateway. Null for successful payments.
     * Examples in real life: INSUFFICIENT_FUNDS, EXPIRED_CARD, FRAUD_DECLINED.
     */
    @Column
    private String failureCode;

    /**
     * Human-readable failure reason. Null for successful payments.
     */
    @Column
    private String failureReason;

    @Column(nullable = false)
    private Instant createdAt;
}