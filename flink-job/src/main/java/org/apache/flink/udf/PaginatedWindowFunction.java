package org.apache.flink.udf;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PaginatedWindowFunction
    extends ProcessWindowFunction<
        Tuple4<String, Long, String, Double>, // Input type
        Tuple3<String, Long, Integer>, // Output type (dateMov, codeAcc, pageNumber)
        String, // Key type (codeAcc)
        TimeWindow> { // Window type

  private final int elementsPerPage;

  public PaginatedWindowFunction(int elementsPerPage) {
    this.elementsPerPage = elementsPerPage;
  }

  @Override
  public void process(
      String codeAcc,
      Context context,
      Iterable<Tuple4<String, Long, String, Double>> elements,
      Collector<Tuple3<String, Long, Integer>> out) {

    List<Tuple4<String, Long, String, Double>> elementList = new ArrayList<>();
    elements.forEach(elementList::add);

    int page = 1;
    int count = 0;

    for (Tuple4<String, Long, String, Double> element : elementList) {
      count++;

      if (count > elementsPerPage) {
        page++;
        count = 1;
      }

      out.collect(new Tuple3<>(codeAcc, element.f1, page)); // (codeAcc, dateMov, pageNumber)
    }
  }
}
