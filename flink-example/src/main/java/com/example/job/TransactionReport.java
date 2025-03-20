package com.example.job;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import com.example.config.Bootstrap;
import com.example.mapper.CustomerAccountMapper;
import com.example.mapper.TransactionMapper;
import com.example.udf.SortProcessFunction;
import com.example.udf.SumWindowFunction;;

public class TransactionReport {
    public static void process(Bootstrap bootstrap) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        String customerPath = bootstrap.inputPath + "/input.txt";
        System.out.println("Processing file: " + customerPath);
        Files.lines(Paths.get(customerPath)).forEach(System.out::println);
        FileSource<String> fileSource = FileSource
            .forRecordStreamFormat(new TextLineInputFormat(), new Path(customerPath))
            .build();
        DataStream<String> customerData = env.fromSource(
            fileSource,
            WatermarkStrategy.noWatermarks(), 
            "source-account"
        );

        System.out.println("RUNNING...");
        // Parse to Customer Account
        DataStream<Tuple3<String, String, String>> customerStream = customerData
                .map(new CustomerAccountMapper())
                .filter(tuple -> tuple != null)
                .filter(tuple -> tuple.f2 != null && tuple.f2.trim().isEmpty()); // Filter by emailAccv where is empty
        customerStream.print();
        DataStream<Tuple3<String, String, String>> customerWaterMarkStream = customerStream
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple3<String, String, String>>forBoundedOutOfOrderness(bootstrap.boundedOutOfOrderness)
                    .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, String>>() {
                        @Override
                        public long extractTimestamp(Tuple3<String, String, String> element, long recordTimestamp) {
                            return Instant.now().toEpochMilli(); // Use current time
                        }
                    })
            );
        customerWaterMarkStream.print();

        System.out.println("TO FILE");
        final FileSink<String> sink = FileSink
                .forRowFormat(new Path(bootstrap.outputPath), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                    DefaultRollingPolicy.builder()
                        .withRolloverInterval(bootstrap.rolloverInterval)
                        .withInactivityInterval(bootstrap.inactivityInterval)
                        .withMaxPartSize(bootstrap.maxPartSize)
                        .build())
                .build();
        customerWaterMarkStream
            .map(tuple -> tuple.f0 + "," + tuple.f1 + "," + tuple.f2)
            .sinkTo(sink);

        env.execute("Transaction Report");
        System.out.println("END...");
    }
}
