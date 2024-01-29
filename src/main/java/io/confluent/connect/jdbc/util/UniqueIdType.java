package io.confluent.connect.jdbc.util;

import java.util.concurrent.atomic.AtomicInteger;

public enum UniqueIdType {
    UUID {
        @Override
        public String generateUniqueId() {
            return java.util.UUID.randomUUID().toString();
        }
    },

    TIMESTAMP_RANDOM {
        @Override
        public String generateUniqueId() {
            long timestamp = System.currentTimeMillis();
            int randomNumber = (int) (Math.random() * 1000);
            return timestamp + "-" + randomNumber;
        }
    },

    TIMESTAMP_MS {
        @Override
        public String generateUniqueId() {
            long timestamp = System.currentTimeMillis();
            return timestamp + "";
        }
    };

    // Abstract method to be implemented by each enum constant
    public abstract String generateUniqueId();

    public static UniqueIdType fromString(String uniqueIdType) {
        if (uniqueIdType != null) {
            for (UniqueIdType b : UniqueIdType.values()) {
                if (uniqueIdType.equalsIgnoreCase(b.name())) {
                    return b;
                }
            }
        }
        return null;
    }
}
