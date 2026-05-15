package com.orderflow.order;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

/**
 * REST API for orders.
 *
 * @RestController = @Controller + @ResponseBody. Methods return Java objects;
 * Spring auto-serializes them to JSON via Jackson (auto-configured because
 * jackson-databind is on the classpath, courtesy of spring-boot-starter-web).
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;
    private final OrderRepository orderRepository;

    @PostMapping
    public ResponseEntity<Order> place(@RequestBody OrderRequest request) throws InterruptedException {
        Order saved = orderService.placeOrder(request);
        return ResponseEntity.ok(saved);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> getById(@PathVariable UUID id) {
        return orderRepository.findById(id)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}