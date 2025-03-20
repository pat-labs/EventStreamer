package com.example.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class CustomerAccountMapper implements MapFunction<String, Tuple3<String, String, String>> {

    // Lista de índices de inicio y fin para cada campo
    private static final List<int[]> INDEXES = Arrays.asList(
        new int[]{0, 10},   // codeCust
        new int[]{10, 18},  // name
        new int[]{18, 30},  // lastName
        new int[]{30, 39},  // homeAddr
        new int[]{39, 60},  // emailAccv
        new int[]{60, 70},  // codeAcc
        new int[]{70, 80}   // user
    );

    @Override
    public Tuple3<String, String, String> map(String value) {
        System.out.println("Input value: " + value);
        return parseByIndex(value).orElse(null);
    }

    private Optional<Tuple3<String, String, String>> parseByIndex(String value) {
        try {
            if (value.length() < 80) {
                System.out.println("Input string is too short: " + value);
                return Optional.empty();
            }

            //Long codeCust = Long.parseLong(value.substring(INDEXES.get(0)[0], INDEXES.get(0)[1]).trim());
            String name = value.substring(INDEXES.get(1)[0], INDEXES.get(1)[1]).trim();
            //String lastName = value.substring(INDEXES.get(2)[0], INDEXES.get(2)[1]).trim();
            //String homeAddr = value.substring(INDEXES.get(3)[0], INDEXES.get(3)[1]).trim();
            String emailAccv = value.substring(INDEXES.get(4)[0], INDEXES.get(4)[1]).trim();
            String codeAcc = value.substring(INDEXES.get(5)[0], INDEXES.get(5)[1]).trim();
            //String user = value.substring(INDEXES.get(6)[0], INDEXES.get(6)[1]).trim();

            return Optional.of(new Tuple3<>(codeAcc, name, emailAccv));
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            System.out.println("Error in parsing value: " + value + " - " + e.getMessage());
            return Optional.empty();
        }
    }
}