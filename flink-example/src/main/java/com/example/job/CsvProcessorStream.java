package com.example.job;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.config.Bootstrap;
import com.example.mapper.TransactionMapper;
import com.example.udf.AvgProcessWindowFunction;
import com.example.udf.SortProcessFunction;

public class CsvProcessorStream {
    private static final Logger logger = LoggerFactory.getLogger(CsvProcessorStream.class);

    public static void process(Bootstrap bootstrap) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String inputFilePath = bootstrap.inputPath + "/input.txt";
        logger.info("Processing file: " + inputFilePath);

        try{
            FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(inputFilePath))
                .build();
            DataStream<String> data = env.fromSource(
                fileSource,
                WatermarkStrategy.noWatermarks(), 
                "source-transactions"
            );
        
            DataStream<Tuple4<String, String, String, Integer>> dataStream = data
                    .map(new TransactionMapper())
                    .filter(tuple -> tuple != null);
            DataStream<Tuple4<String, String, String, Integer>> watermarkDataStream = dataStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple4<String, String, String, Integer>>forBoundedOutOfOrderness(bootstrap.boundedOutOfOrderness)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String, String, String, Integer>>() {
                        @Override
                        public long extractTimestamp(Tuple4<String, String, String, Integer> element, long recordTimestamp) {
                            return Long.parseLong(element.f0);
                        }
                    })
            );

            DataStream<Tuple4<String, String, String, Double>> averageStream = averageStream(watermarkDataStream, bootstrap);

            final FileSink<String> sink = FileSink
                .forRowFormat(new Path(bootstrap.outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                    DefaultRollingPolicy.builder()
                        .withRolloverInterval(bootstrap.rolloverInterval)
                        .withInactivityInterval(bootstrap.inactivityInterval)
                        .withMaxPartSize(bootstrap.maxPartSize)
                        .build())
                .build();
            averageStream
                .map(tuple -> tuple.f0 + "," + tuple.f1 + "," + tuple.f2 + "," + tuple.f3)
                .sinkTo(sink);
        } catch (Throwable t) {
            logger.error("ERROR: " + t);
        }

        env.execute("TXT to CSV Converter using DataStream API");
    }

    public static DataStream<Tuple4<String, String, String, Integer>> sortStream(
            DataStream<Tuple4<String, String, String, Integer>> watermarkDataStream) {

        return watermarkDataStream
            .keyBy(value -> value.f1)
            .process(new SortProcessFunction());
    }

    public static DataStream<Tuple4<String, String, String, Double>> averageStream(
        DataStream<Tuple4<String, String, String, Integer>> watermarkDataStream, Bootstrap bootstrap) throws Exception {

        DataStream<Tuple4<String, String, String, Double>> windowedData = watermarkDataStream
                .keyBy(tuple -> tuple.f1 + "-" + tuple.f2)
                .window(TumblingEventTimeWindows.of(bootstrap.tumblingWindowSize))
                .process(new AvgProcessWindowFunction());

        return windowedData;
    }
}
