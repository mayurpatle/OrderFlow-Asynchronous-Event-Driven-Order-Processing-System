package com.orderflow.order;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * HTTP API for orders.
 *
 * Notice the response: 202 Accepted, not 200 OK. This is intentional.
 *   - 200 OK means "your request is complete and here's the result."
 *   - 202 Accepted means "I received your request and will process it
 *     asynchronously."
 *
 * In the microservices version, the order is saved synchronously (so we have
 * a record), but the downstream work (inventory, payment, shipping, notification)
 * happens asynchronously. The customer should know to poll GET /api/orders/{id}
 * to see the final state, or we'll push the state to them via SSE/websockets later.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;

    @PostMapping
    public ResponseEntity<OrderEntity> place(@RequestBody OrderRequest request) {
        OrderEntity saved = orderService.placeOrder(request);
        return ResponseEntity.accepted().body(saved);
    }

    @GetMapping("/{id}")
    public ResponseEntity<OrderEntity> getById(@PathVariable UUID id) {
        return orderRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
