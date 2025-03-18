package com.example;

import java.time.Duration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

import com.example.config.Bootstrap;
import com.example.config.EnvLoader;

public class SocketToCsv {
    public static void main(String[] args) throws Exception {
        EnvLoader envLoader = new EnvLoader();
        Bootstrap bootstrap = envLoader.buildBootstrap();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<String> textStream = env.socketTextStream(bootstrap.socketHostname, bootstrap.socketPort, "\n");

        DataStream<Tuple2<String, Long>> windowCounts = wordCountStream(textStream);

        FileSink<String> sink = FileSink
            .forRowFormat(new Path(bootstrap.outputPath + "/output.csv"), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofMinutes(5))
                    .withInactivityInterval(Duration.ofMinutes(2))
                    .withMaxPartSize(MemorySize.ofMebiBytes(512))
                    .build()
            ).build();

        windowCounts
            .map(tuple -> tuple.f0 + "," + tuple.f1)
            .sinkTo(sink);

        env.execute("Socket to CSV Word Count");
    }

    public static DataStream<Tuple2<String, Long>> wordCountStream(DataStream<String> input) {
        KeyedStream<Tuple2<String, Long>, String> keyedStream = input
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0);

        return keyedStream
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(2)))
            // Trigger on 2 events
            //.trigger(PurgingTrigger.of(CountTrigger.of(2)))
            .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));
    }

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
            for (String word : value.split("\\s+")) {
                out.collect(Tuple2.of(word, 1L));
            }
        }
    }
}
