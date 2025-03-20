package com.example.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class SumWindowFunction extends ProcessWindowFunction<
        Tuple4<Long, String, String, Double>,
        Tuple5<Long, String, String, Double, Double>,
        String, // key is codeAcc + dateMov
        TimeWindow> {

    @Override
    public void process(
            String key,
            Context context,
            Iterable<Tuple4<Long, String, String, Double>> elements,
            Collector<Tuple5<Long, String, String, Double, Double>> out) {

        double sum = 0.0;
        double amount = 0.0;
        long dateMov = 0;
        String codeAcc = key;
        String descMov = "";

        List<Tuple4<Long, String, String, Double>> elementList = new ArrayList<>();
        for (Tuple4<Long, String, String, Double> event : elements) {
            sum += event.f3;
            dateMov = event.f0;
            codeAcc = event.f1;
            descMov = event.f2;
            amount = event.f3;
            elementList.add(event);
        }

        out.collect(new Tuple5<>(dateMov, codeAcc, descMov, amount, sum));
    }
}
