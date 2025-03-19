package com.example.utils;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CollectSink<T> implements SinkFunction<T> {

    public static final List<Object> values = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void invoke(T value, Context context) {
        values.add(value);
    }

    @SuppressWarnings("unchecked")
    public static <T> List<T> getResults() {
        List<T> result = new ArrayList<>();
        for (Object obj : values) {
            try {
                result.add((T) obj);
            } catch (ClassCastException e) {
                // Handle the case where the object is not of the expected type
            }
        }
        return result;
    }

    public static void clear() {
        values.clear();
    }
}
