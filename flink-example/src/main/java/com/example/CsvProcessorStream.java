package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.utils.PersonParser;

import java.time.Duration;

public class CsvProcessorStream {
    public static void main(String[] args) throws Exception {
        String inputPath = "/opt/flink/data/input.txt";
        String outputPath = "/opt/flink/data/output";

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> source = buildDataStream(inputPath, env);
        FileSink<String> sink = buildSink(outputPath);

        proccessStream(source, sink);

        env.execute("TXT to CSV Converter using DataStream API");
    }

    public static class SumProcessWindowFunction extends ProcessWindowFunction<
        Tuple4<Integer, String, String, Integer>,
        Tuple4<Integer, String, String, Integer>,
        Integer,                                 
        TimeWindow> {                            

        @Override
        public void process(
            Integer key,
            Context context,
            Iterable<Tuple4<Integer, String, String, Integer>> elements,
            Collector<Tuple4<Integer, String, String, Integer>> out) {
            
            int sum = 0;
            String firstName = "";
            String lastName = "";

            for (Tuple4<Integer, String, String, Integer> event : elements) {
                System.out.println("Processing window: id:" + event.f0 +
                    " amount: " + event.f3 + " ts: " + context.currentWatermark());

                sum += event.f3;
                firstName = event.f1;
                lastName = event.f2;
            }

            out.collect(new Tuple4<>(key, firstName, lastName, sum));
        }
    }

    public static void proccessStream(DataStream<String> data, FileSink<String> sink) throws Exception{
        DataStream<Tuple4<Integer, String, String, Integer>> dataStream = data
            .map(new PersonParser())
            .filter(tuple -> tuple != null);

        DataStream<Tuple4<Integer, String, String, Integer>> windowedData = dataStream
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
            .process(new SumProcessWindowFunction());

        windowedData
            .map(tuple -> tuple.f0 + "," + tuple.f1 + "," + tuple.f2 + "," + tuple.f3)
            .sinkTo(sink);
    }

    private static FileSink<String> buildSink(String outputPath) {
        final FileSink<String> sink = FileSink
            .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(Duration.ofSeconds(5))
                    .withInactivityInterval(Duration.ofSeconds(3))
                    .withMaxPartSize(MemorySize.ofMebiBytes(1))
                    .build())
            .build();
        return sink;
    }

    private static DataStream<String> buildDataStream(String inputPath, StreamExecutionEnvironment env) {
        FileSource<String> fileSource = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
            .build();
        DataStream<String> dataStream = env.fromSource(
            fileSource,
                //WatermarkStrategy.noWatermarks(), 
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(6))
                    .withTimestampAssigner((event, timestamp) -> 
                        System.currentTimeMillis()
                    ),
                "source-persons"
            );
        return dataStream;
    }
}
