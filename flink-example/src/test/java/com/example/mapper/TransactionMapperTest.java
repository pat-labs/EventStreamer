package com.example.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.Test;

import com.example.mapper.TransactionMapper;

public class TransactionMapperTest {
    @Test
    public void testValidInput() throws Exception {
        TransactionMapper parser = new TransactionMapper();
        Tuple4<String, String, String, Integer> result = parser.map("1652088888997,1,1,30");
        assertNotNull(result);
        assertEquals("1652088888997", result.f0);
        assertEquals("1", result.f1);
        assertEquals("1", result.f2);
        assertEquals(30, result.f3);
    }

}
