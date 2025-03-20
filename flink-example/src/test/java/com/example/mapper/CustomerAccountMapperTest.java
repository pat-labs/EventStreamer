package com.example.mapper;

import org.apache.flink.api.java.tuple.Tuple3;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

public class CustomerAccountMapperTest {

    @Test
    public void testMapFunction() {
        CustomerAccountMapper mapper = new CustomerAccountMapper();
        String testData = "0864774132Charley   CruickshanNorth Anibmabel.terry@hotmail.0000987420CCruicksha";

        Tuple3<String, String, String> result = mapper.map(testData);

        assertNotNull(result, "The mapping function should not return null.");

        // Validación de cada campo basado en los índices correctos
        assertEquals("0000987420", result.f0, "Account code should match.");
        assertEquals("Charley", result.f1, "First name should match.");
        assertEquals("bmabel.terry@hotmail.", result.f2, "Email should match.");
        //assertEquals(864774132L, result.f0, "Customer code should match.");
        //assertEquals("Cruickshan", result.f2, "Last name should match.");
        //assertEquals("North Ani", result.f3, "Home address should match.");
        //assertEquals("CCruicksha", result.f6, "User should match.");
    }
}

