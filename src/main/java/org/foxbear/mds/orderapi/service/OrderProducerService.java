package org.foxbear.mds.orderapi.service;

import lombok.extern.slf4j.Slf4j;
import org.foxbear.mds.model.OrderEvent;
import org.springframework.stereotype.Service;
import org.springframework.kafka.core.KafkaTemplate;

@Service
@Slf4j
public class OrderProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private static final String TOPIC = "order-events";

    public OrderProducerService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendOrder(OrderEvent event) {
        log.info("Slanje porudžbine na Kafku. ID: {}", event.orderId());
        kafkaTemplate.send(TOPIC, event.orderId(), event);
    }
}
