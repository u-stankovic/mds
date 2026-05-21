package org.foxbear.mds.inventory.service;

import lombok.extern.slf4j.Slf4j;
import org.foxbear.mds.model.OrderEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class InventoryConsumerService {

    private final Map<String, Integer> inventoryStock = new ConcurrentHashMap<>();
    private final Map<String, Boolean> processedOrders = new ConcurrentHashMap<>();

    public InventoryConsumerService() {
        inventoryStock.put("item-1", 280);
        inventoryStock.put("item-2", 150);
    }

    @KafkaListener(topics = "order-events", groupId = "inventory-group")
    public void processOrder(OrderEvent event) {
        log.info("Konzumer primio porudžbinu za obradu. ID: {}", event.orderId());

        if (processedOrders.containsKey(event.orderId())) {
            log.warn("Idempotency aktiviran: Porudžbina {} je već obrađena.", event.orderId());
            return;
        }

        Integer currentStock = inventoryStock.getOrDefault(event.itemId(), 0);

        if (currentStock >= event.quantity()) {
            inventoryStock.put(event.itemId(), currentStock - event.quantity());
            processedOrders.put(event.orderId(), true);
            log.info("Uspeh: Porudžbina {} odobrena. Artikal: {}, Kupljeno: {}, Preostalo na lageru: {}",
                    event.orderId(), event.itemId(), event.quantity(), inventoryStock.get(event.itemId()));
        } else {
            log.warn("Odbijeno: Nema dovoljno zaliha za porudžbinu {}. Traženo: {}, Na lageru: {}",
                    event.orderId(), event.quantity(), currentStock);
            processedOrders.put(event.orderId(), true);
        }
    }
}
