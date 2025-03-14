package com.example;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.example.config.Bootstrap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvProcessorStreamTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(1)
            .setNumberTaskManagers(2)
            .build()
    );

    @Test
    void testCsvProcessorStream() throws Exception {
        Bootstrap bootstrap = new Bootstrap();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  
        env.setParallelism(1);

        CollectSink.values.clear();

        //tiemstamp,person_id, type_transaction, ammount_spend
        List<String> input = Arrays.asList(
            "1652088888997,1,1,30",
            "1752088888997,2,2,25",
            "1752088888997,3,1,40",
            "1652088888997,1,1,20",  // Se promedia con la otra transacción del mismo grupo
            "1552088888997,4,2,25",
            "1552088888997,1,2,10"
        );
        
        DataStream<String> dataStream = env.fromData(input);

        DataStream<Tuple4<String, String, String, Double>> resultStream =
            CsvProcessorStream.proccessStream(dataStream, bootstrap);
        resultStream.print();

        resultStream.addSink(new CollectSink());
        env.execute();

        List<Tuple4<String, String, String, Double>> results = CollectSink.values;
        assertTrue(results.contains(new Tuple4<>("1", "1652088888997", "1", 25.0)));
    }

    private static class CollectSink implements SinkFunction<Tuple4<String, String, String, Double>> {
        public static final List<Tuple4<String, String, String, Double>> values =
                Collections.synchronizedList(new ArrayList<>());
    
        @Override
        public void invoke(Tuple4<String, String, String, Double> value, Context context) {
            values.add(value);
        }
    }
}
