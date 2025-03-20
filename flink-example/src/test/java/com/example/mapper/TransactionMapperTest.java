package com.example.mapper;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import org.apache.flink.api.java.tuple.Tuple5;

public class TransactionMapperTest {
    @Test
    public void testMapFunction() {
        // TransactionMapper mapper = new TransactionMapper();
        // String testData = "2025 03.20 14:07:37           00009290230000835322Mung Beans          66.84      ";

        // Tuple5<Long, String, String, String, Double> result = mapper.map(testData);

        // assertNotNull(result, "The mapping function should not return null.");

        // LocalDateTime expectedDate = LocalDateTime.of(2025, 3, 20, 14, 7, 37);
        // long expectedMillis = expectedDate.toInstant(ZoneOffset.UTC).toEpochMilli();
        // assertEquals(expectedMillis, result.f0, "Timestamp should match.");

        // assertEquals("0000929023", result.f1, "Account code should match.");
        // assertEquals("0000835322", result.f2, "Movement code should match.");
        // assertEquals("Mung Beans", result.f3, "Description should match.");
        // assertEquals(66.84, result.f4, 0.01, "Amount should match.");
    }
}
