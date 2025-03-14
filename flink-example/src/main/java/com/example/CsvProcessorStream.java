package com.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.example.config.Bootstrap;
import com.example.utils.TransactionParser;

public class CsvProcessorStream {
    public static void main(String[] args) throws Exception {
        final String inputPath;
        final String outputPath;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            inputPath = params.get("inputPath", "/opt/flink/data/input.txt"); // Default input path
            outputPath = params.get("outputPath", "/opt/flink/data/output"); // Default output path
        } catch (Exception e) {
            System.err.println("Usage:--inputPath <input-path> --outputPath <output-path>");
            return;
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> fileSource = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputPath))
            .build();
        DataStream<String> dataStream = env.fromSource(
            fileSource,
            WatermarkStrategy.noWatermarks(), 
            "source-transactions"
        );

        Bootstrap bootstrap = new Bootstrap();

        DataStream<Tuple4<String, String, String, Double>> result = proccessStream(dataStream, bootstrap);

        FileSink<String> sink = buildSink(outputPath, bootstrap);
        result
            .map(tuple -> tuple.f0 + "," + tuple.f1 + "," + tuple.f2 + "," + tuple.f3)
            .sinkTo(sink);

        env.execute("TXT to CSV Converter using DataStream API");
    }

    public static class AvgProcessWindowFunction extends ProcessWindowFunction<
        Tuple4<String, String, String, Integer>,
        Tuple4<String, String, String, Double>, // Usamos Double para el promedio
        String, // Key será "person_id-type_transaction"
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
                System.out.println("Processing window: id:" + event.f1 +
                        " amount: " + event.f3 + " ts: " + context.currentWatermark());

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

    public static DataStream<Tuple4<String, String, String, Double>> proccessStream(
        DataStream<String> data, Bootstrap bootstrap) throws Exception {

        DataStream<Tuple4<String, String, String, Integer>> dataStream = data
                .map(new TransactionParser())
                .filter(tuple -> tuple != null);

        DataStream<Tuple4<String, String, String, Integer>> watermarkDataStream = dataStream
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.<Tuple4<String, String, String, Integer>>forBoundedOutOfOrderness(bootstrap.boundedOutOfOrderness)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Integer>>() {
                            @Override
                            public long extractTimestamp(Tuple4<String, String, String, Integer> element, long recordTimestamp) {
                                return Long.parseLong(element.f0); // Extract timestamp from f0
                            }
                        })
                );

        DataStream<Tuple4<String, String, String, Double>> windowedData = watermarkDataStream
                .keyBy(tuple -> tuple.f1 + "-" + tuple.f2) // Group by person_id + type_transaction
                .window(TumblingEventTimeWindows.of(bootstrap.tumblingWindowSize))
                .process(new AvgProcessWindowFunction());

        return windowedData;
    }


    private static FileSink<String> buildSink(String outputPath, Bootstrap bootstrap) {
        final FileSink<String> sink = FileSink
            .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(bootstrap.rolloverInterval)
                    .withInactivityInterval(bootstrap.inactivityInterval)
                    .withMaxPartSize(bootstrap.maxPartSize)
                    .build())
            .build();
        return sink;
    }
}
