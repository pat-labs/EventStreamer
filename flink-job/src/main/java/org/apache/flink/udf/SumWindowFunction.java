package org.apache.flink.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SumWindowFunction
    extends ProcessWindowFunction<
        Tuple4<String, Long, String, Double>, // Input (codeAcc, dateMov, codeMov, descMov, amount)
        Tuple2<String, Double>, // Output (codeAcc, sum)
        String, // key is codeAcc
        TimeWindow> {

  @Override
  public void process(
      String codeAcc,
      Context context,
      Iterable<Tuple4<String, Long, String, Double>> elements,
      Collector<Tuple2<String, Double>> out) {

    double sum = 0.0;

    for (Tuple4<String, Long, String, Double> event : elements) {
      sum += event.f3;
    }

    out.collect(new Tuple2<>(codeAcc, sum));
  }
}
