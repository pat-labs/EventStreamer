package com.example.udf;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.example.config.Bootstrap;
import com.example.config.EnvLoader;
import com.example.job.CsvProcessorStream;
import com.example.mapper.TransactionMapper;
import com.example.utils.CollectSink;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AverageProcessTest {
    private StreamExecutionEnvironment env;
    private DataStream<Tuple4<String, String, String, Integer>> dataStream;
    private Bootstrap bootstrap;

    @BeforeEach
    void setUp() throws Exception {
        bootstrap = new EnvLoader("././.env.dev").buildBootstrap();

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(bootstrap.parallelism);

        List<String> input = Arrays.asList(
                "1652088888992,1,1,30",
                "1752088888996,2,2,25",
                "1752088888995,3,1,40",
                "1652088888994,1,1,20",
                "1552088888997,4,2,25",
                "1552088888991,1,2,10"
        );

        DataStream<String> data = env.fromData(input);

        dataStream = data
                .map(new TransactionMapper())
                .filter(tuple -> tuple != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple4<String, String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Integer>>() {
                                    @Override
                                    public long extractTimestamp(Tuple4<String, String, String, Integer> element, long recordTimestamp) {
                                        return Long.parseLong(element.f0);
                                    }
                                })
                );
    }

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(1)
            .setNumberTaskManagers(2)
            .build()
    );

    @Test
    void testCsvAverageStream() throws Exception {
        CollectSink.clear();

        DataStream<Tuple4<String, String, String, Double>> resultStream =
                CsvProcessorStream.averageStream(dataStream, bootstrap);

        resultStream.addSink(new CollectSink<>());
        env.execute();

        List<Tuple4<String, String, String, Double>> results = CollectSink.getResults();
        assertTrue(results.contains(new Tuple4<>("1", "1652088888994", "1", 25.0)));
    }
}
