package org.apache.flink.mapper;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;

public class TransactionMapper
    implements MapFunction<String, Tuple4<String, Long, String, Double>> {

  // Formato de fecha esperado en el campo dateMov
  private static final DateTimeFormatter DATE_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy MM.dd mm:HH:ss");

  // Lista de índices de inicio y fin para cada campo
  private static final List<int[]> INDEXES =
      Arrays.asList(
          new int[] {0, 19}, // dateMov
          new int[] {30, 40}, // codeAcc
          new int[] {40, 50}, // codeMov
          new int[] {50, 70}, // descMov
          new int[] {70, 80} // amount
          );

  @Override
  public Tuple4<String, Long, String, Double> map(String value) {
    return parseByIndex(value).orElse(null);
  }

  private Optional<Tuple4<String, Long, String, Double>> parseByIndex(String value) {
    try {
      if (value.length() < 80) {
        System.out.println("Input string is too short: " + value);
        return Optional.empty();
      }

      String dateMovStr = value.substring(INDEXES.get(0)[0], INDEXES.get(0)[1]).trim();
      String codeAcc = value.substring(INDEXES.get(1)[0], INDEXES.get(1)[1]).trim();
      // String codeMov = value.substring(INDEXES.get(2)[0], INDEXES.get(2)[1]).trim();
      String descMov = value.substring(INDEXES.get(3)[0], INDEXES.get(3)[1]).trim();
      Double amount = Double.parseDouble(value.substring(INDEXES.get(4)[0]).trim());
      if (amount < 0.0) {
        throw new NumberFormatException("Negative Amount");
      }

      // Convertir dateMov a timestamp en milisegundos
      Long dateMov = parseDateToMillis(dateMovStr);

      return Optional.of(new Tuple4<>(codeAcc, dateMov, descMov, amount));
    } catch (NumberFormatException | StringIndexOutOfBoundsException e) {
      System.out.println("Error parsing value: " + value + "-" + e.getMessage());
      return Optional.empty();
    }
  }

  private Long parseDateToMillis(String dateStr) {
    try {
      LocalDateTime dateTime = LocalDateTime.parse(dateStr, DATE_FORMATTER);
      return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    } catch (Exception e) {
      System.out.println("Invalid date format: " + dateStr + "-" + e.getMessage());
      throw new RuntimeException("Invalid date format: " + dateStr, e);
    }
  }
}
