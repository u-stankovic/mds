package org.foxbear.mds.orderapi.controller;

import org.foxbear.mds.model.OrderEvent;
import org.foxbear.mds.orderapi.service.OrderProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderProducerService producerService;

    public OrderController(OrderProducerService producerService) {
        this.producerService = producerService;
    }

    @PostMapping
    public ResponseEntity<Void> createOrder(@RequestBody OrderEvent event) {

        if (event.orderId() == null || event.itemId() == null || event.quantity() <= 0) {
            return ResponseEntity.badRequest().build();
        }

        producerService.sendOrder(event);

        return ResponseEntity.status(HttpStatus.ACCEPTED).build();
    }


}
