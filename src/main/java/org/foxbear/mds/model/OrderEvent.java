package org.foxbear.mds.model;

public record OrderEvent(
        String orderId,
        String itemId,
        int quantity
) { }
