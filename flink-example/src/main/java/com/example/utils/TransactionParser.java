package com.example.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

import java.util.Optional;

public class TransactionParser implements MapFunction<String, Tuple4<String, String, String, Integer>> {

    @Override
    public Tuple4<String, String, String, Integer> map(String value) {
        return parseTransaction(value).orElse(null);
    }

    private Optional<Tuple4<String, String, String, Integer>> parseTransaction(String value) {
        return parseTransactionByDelimiter(value, ",")
                .or(() -> parseTransactionByIndex(value, 0, 15, 17, 18));
    }

    private Optional<Tuple4<String, String, String, Integer>> parseTransactionByDelimiter(String value, String delimiter) {
        String[] parts = value.split(delimiter);
        if (parts.length == 4) {
            try {
                String createAt = parts[0].trim();
                String personId = parts[1].trim();
                String typeTransaction = parts[2].trim();
                int ammount = Integer.parseInt(parts[3].trim());
                return Optional.of(new Tuple4<>(createAt, personId, typeTransaction, ammount));
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    private Optional<Tuple4<String, String, String, Integer>> parseTransactionByIndex(String value, int firstIndex, int secondIndex, int thirdIndex, int fourthIndex) {
        try {
            String createAt = value.substring(firstIndex, secondIndex).trim();
            String personId = value.substring(secondIndex, thirdIndex).trim();
            String typeTransaction = value.substring(thirdIndex, fourthIndex).trim();
            int ammount = Integer.parseInt(value.substring(fourthIndex).trim());
            return Optional.of(new Tuple4<>(createAt, personId, typeTransaction, ammount));
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            return Optional.empty();
        }
    }
}