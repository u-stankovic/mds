package org.foxbear.mds.orderapi.controller;

import lombok.extern.slf4j.Slf4j;
import org.foxbear.mds.model.OrderEvent;
import org.foxbear.mds.orderapi.service.OrderProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.concurrent.CompletableFuture;

@RestController
@RequestMapping("/orders")
@Slf4j
public class OrderController {

    private final OrderProducerService producerService;

    public OrderController(OrderProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<Void> createOrder(@RequestBody OrderEvent event) {

        if (event.orderId() == null || event.itemId() == null) {
            log.warn("Nevalidan zahtev: orderId i itemId ne smeju biti null.");
            return ResponseEntity.badRequest().build();
        }

        if (event.quantity() <= 0) {
            log.warn("Nevalidna količina za porudžbinu {}: prosleđena vrednost je {}.", event.orderId(), event.quantity());
            return ResponseEntity.badRequest().build();
        }

        producerService.sendOrder(event);

        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }

    @PostMapping("/stress-test")
    public ResponseEntity<String> runStressTest(@RequestParam(defaultValue = "20") int count) {
        log.info("Započinjem stress test: Generišem {} porudžbina...", count);

        CompletableFuture.runAsync(() -> {
            for (int i = 1; i <= count; i++) {
                String uniqueOrderId = "STRESS-" + System.currentTimeMillis() + "-" + i;
                OrderEvent testEvent = new OrderEvent(uniqueOrderId, "item-1", 2);
                producerService.sendOrder(testEvent);
            }
            log.info("Stress test uspešno završen. Poslato {} poruka.", count);
        });

        return ResponseEntity.ok("Stress test pokrenut u pozadini za " + count + " poruka.");
    }
}
