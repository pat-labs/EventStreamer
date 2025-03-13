package com.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.flink.api.java.tuple.Tuple4;
import org.junit.jupiter.api.Test;

import com.example.utils.PersonParser;

public class PersonParserTest {
    @Test
    public void testValidInput() throws Exception {
        PersonParser parser = new PersonParser();
        Tuple4<Integer, String, String, Integer> result = parser.map("123-John-Doe-30");
        assertNotNull(result);
        assertEquals(123, result.f0);
        assertEquals("John", result.f1);
        assertEquals("Doe", result.f2);
        assertEquals(30, result.f3);
    }

}
