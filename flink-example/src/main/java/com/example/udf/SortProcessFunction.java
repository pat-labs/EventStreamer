package com.example.udf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SortProcessFunction extends KeyedProcessFunction<String, Tuple4<String, String, String, Integer>, Tuple4<String, String, String, Integer>> {
    private static final Logger logger = LoggerFactory.getLogger(SortProcessFunction.class);

    private transient ListState<Tuple4<String, String, String, Integer>> bufferState;

    @Override
    public void open(OpenContext openContext) throws Exception {
        ListStateDescriptor<Tuple4<String, String, String, Integer>> descriptor =
                new ListStateDescriptor<>("bufferState", org.apache.flink.api.common.typeinfo.Types.TUPLE(
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.STRING,
                        org.apache.flink.api.common.typeinfo.Types.INT
                ));
        bufferState = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void processElement(
            Tuple4<String, String, String, Integer> value,
            Context ctx,
            Collector<Tuple4<String, String, String, Integer>> out) throws Exception {

        bufferState.add(value);
        logger.info("🟢 Element added to state: " + value);

        long eventTime = Long.parseLong(value.f0); // Convert timestamp from String to long
        long triggerTime = eventTime + 1; // Schedule timer one millisecond later

        logger.info("🕒 Timer registered for: " + triggerTime);
        ctx.timerService().registerEventTimeTimer(triggerTime); // Register the event time timer
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
        logger.info("⏰ Timer triggered at timestamp: " + timestamp);

        List<Tuple4<String, String, String, Integer>> sortedList = new ArrayList<>();
        for (Tuple4<String, String, String, Integer> item : bufferState.get()) {
            sortedList.add(item);
        }

        sortedList.sort(Comparator.comparing(t -> t.f0)); // Sort by first field

        logger.info("📌 Sorted list:");
        for (Tuple4<String, String, String, Integer> item : sortedList) {
            out.collect(item);
        }

        bufferState.clear();
    }
}
