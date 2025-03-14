package com.example;

import java.time.Duration;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

public class SocketToCsv {
    public static void main(String[] args) throws Exception {
        final String hostname;
        final int port;

        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            hostname = params.has("hostname") ? params.get("hostname") : "localhost";
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("Usage: --hostname <hostname> --port <port>");
            return;
        }

        String outputPath = "/opt/flink/data/output.csv";

        execSocket(hostname, port, outputPath);
    }

    public static void execSocket(String hostname, int port, String outputPath) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> textStream = env.socketTextStream(hostname, port, "\n");

        KeyedStream<Tuple2<String, Long>, String> keyedStream = textStream
            .flatMap(new Tokenizer())
            .keyBy(value -> value.f0);

        DataStream<Tuple2<String, Long>> windowCounts = keyedStream
            .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
            .reduce((a, b) -> Tuple2.of(a.f0, a.f1 + b.f1));

        FileSink<String> sink = FileSink
            .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
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

    public static class Tokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
            for (String word : value.split("\\s+")) {
                out.collect(Tuple2.of(word, 1L));
            }
        }
    }
}
