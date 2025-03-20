package com.example.mapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class TransactionMapper implements MapFunction<String, Tuple4<Long, String, String, Double>> {
    private static final Logger logger = LoggerFactory.getLogger(TransactionMapper.class);

    // Formato de fecha esperado en el campo dateMov
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy MM.dd HH:mm:ss");

    // Lista de índices de inicio y fin para cada campo
    private static final List<int[]> INDEXES = Arrays.asList(
        new int[]{0, 19},   // dateMov
        new int[]{30, 40},  // codeAcc
        new int[]{40, 50},  // codeMov
        new int[]{50, 70},  // descMov
        new int[]{70, 80}   // amount (ajustado para evitar errores de parseo)
    );

    @Override
    public Tuple4<Long, String, String, Double> map(String value) {
        return parseByIndex(value).orElse(null);
    }

    private Optional<Tuple4<Long, String, String, Double>> parseByIndex(String value) {
        try {
            String dateMovStr = value.substring(INDEXES.get(0)[0], INDEXES.get(0)[1]).trim();
            String codeAcc = value.substring(INDEXES.get(1)[0], INDEXES.get(1)[1]).trim();
            //String codeMov = value.substring(INDEXES.get(2)[0], INDEXES.get(2)[1]).trim();
            String descMov = value.substring(INDEXES.get(3)[0], INDEXES.get(3)[1]).trim();
            Double amount = Double.parseDouble(value.substring(INDEXES.get(4)[0]).trim());

            // Convertir dateMov a timestamp en milisegundos
            Long dateMov = parseDateToMillis(dateMovStr);

            return Optional.of(new Tuple4<>(dateMov, codeAcc, descMov, amount));
        } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
            logger.error("Error parsing value: {} - {}", value, e.getMessage());
            return Optional.empty();
        }
    }

    private Long parseDateToMillis(String dateStr) {
        try {
            LocalDateTime dateTime = LocalDateTime.parse(dateStr, DATE_FORMATTER);
            return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (Exception e) {
            logger.error("Invalid date format: {} - {}", dateStr, e.getMessage());
            throw new RuntimeException("Invalid date format: " + dateStr, e);
        }
    }
}