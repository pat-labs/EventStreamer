package org.apache.flink.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MaxPageWindowFunction
    extends ProcessWindowFunction<
        Tuple3<String, Long, Integer>, // Input (dateMov, codeAcc, pageNumber)
        Tuple2<String, Integer>, // Output (codeAcc, maxPage)
        String, // Key (codeAcc)
        TimeWindow> {

  @Override
  public void process(
      String codeAcc,
      Context context,
      Iterable<Tuple3<String, Long, Integer>> elements,
      Collector<Tuple2<String, Integer>> out) {

    int maxPage = 0;

    for (Tuple3<String, Long, Integer> event : elements) {
      maxPage = Math.max(maxPage, event.f2);
    }

    out.collect(new Tuple2<>(codeAcc, maxPage));
  }
}
