package com.example.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Optional;

public class PersonParser implements MapFunction<String, Tuple4<Integer, String, String, Integer>> {

    @Override
    public Tuple4<Integer, String, String, Integer> map(String value) {
        return parsePerson(value).orElse(null);
    }

    private Optional<Tuple4<Integer, String, String, Integer>> parsePerson(String value) {
        return parsePersonByDelimiter(value, ",")
                .or(() -> parsePersonByIndex(value, 0, 5, 15, 25)); // Example indices, adjust as needed
    }

    private Optional<Tuple4<Integer, String, String, Integer>> parsePersonByDelimiter(String value, String delimiter) {
        String[] parts = value.split(delimiter);
        if (parts.length == 4) {
            try {
                int personId = Integer.parseInt(parts[0].trim());
                String name = parts[1].trim();
                String lastName = parts[2].trim();
                int age = Integer.parseInt(parts[3].trim());
                return Optional.of(new Tuple4<>(personId, name, lastName, age));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    private Optional<Tuple4<Integer, String, String, Integer>> parsePersonByIndex(String value, int firstIndex, int secondIndex, int thirdIndex, int fourthIndex) {
        try {
            int personId = Integer.parseInt(value.substring(firstIndex, secondIndex).trim());
            String name = value.substring(secondIndex, thirdIndex).trim();
            String lastName = value.substring(thirdIndex, fourthIndex).trim();
            int age = Integer.parseInt(value.substring(fourthIndex).trim());
            return Optional.of(new Tuple4<>(personId, name, lastName, age));
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            return Optional.empty();
        }
    }
}