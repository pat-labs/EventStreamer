package com.example.udf;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class AvgProcessWindowFunction extends ProcessWindowFunction<
        Tuple4<String, String, String, Integer>,
        Tuple4<String, String, String, Double>, // Output uses Double for average
        String, // Key is "person_id-type_transaction"
        TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<Tuple4<String, String, String, Integer>> elements,
            Collector<Tuple4<String, String, String, Double>> out) {

        int sum = 0;
        int count = 0;
        String createAt = "";
        String typeTransaction = "";
        String personId = "";

        for (Tuple4<String, String, String, Integer> event : elements) {
            sum += event.f3;
            count++;
            createAt = event.f0;
            personId = event.f1;
            typeTransaction = event.f2;
        }

        double average = (count == 0) ? 0.0 : (double) sum / count;
        out.collect(new Tuple4<>(personId, createAt, typeTransaction, average));
    }
}
