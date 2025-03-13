package com.example;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.test.junit5.MiniClusterExtension;

import com.example.utils.PersonParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class CsvProcessorStreamTest {

    @RegisterExtension
    static final MiniClusterExtension MINI_CLUSTER = new MiniClusterExtension(
        new MiniClusterResourceConfiguration.Builder()
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build()
    );

    private static FileSink<String> buildSink(String outputPath) {
        return FileSink
        .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
        .withRollingPolicy(
            DefaultRollingPolicy.builder()
                .withRolloverInterval(Duration.ofSeconds(1)) 
                .withInactivityInterval(Duration.ofSeconds(1)) 
                .withMaxPartSize(MemorySize.ofMebiBytes(1))
                .build())
        .withOutputFileConfig(OutputFileConfig.builder()
            .withPartPrefix("output")
            .withPartSuffix(".txt")
            .build())
        .build();
    }

    private static DataStream<String> buildDataStream(StreamExecutionEnvironment env) {
        List<String> input = Arrays.asList(
            "1,John,Doe,10",
            "1,John,Doe,15",
            "2,Jane,Smith,20",
            "2,Jane,Smith,25"
        );
        DataStream<String> dataStream = env.fromData(input);
        return dataStream;
    }

    @Test
    void testCsvProcessorStream() throws Exception {
        String currentDir = System.getProperty("user.dir");
        int lastSlashIndex = currentDir.lastIndexOf('/');
        String outputPath = currentDir.substring(0, lastSlashIndex) + "/data/output";
        FileSink<String> sink = buildSink(outputPath);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();  
        env.setParallelism(1);
        DataStream<String> dataStream = buildDataStream(env);

        System.out.println("Processing..." + dataStream);
        CsvProcessorStream.proccessStream(dataStream, sink);
        System.out.println("End process");
        
        File dir = new File(outputPath);
        System.out.println("Checking output directory: " + dir.getAbsolutePath());
        int retries = 30;
        while (retries > 0 && Files.list(dir.toPath()).noneMatch(Files::isRegularFile)) {
            System.out.println("Waiting for output file...");
            Thread.sleep(1000); // Check every second
            retries--;
        }
        List<String> outputLines = Files.walk(dir.toPath())
            .filter(Files::isRegularFile)
            .flatMap(path -> {
                try {
                    return Files.lines(path);
                } catch (Exception e) {
                    return Stream.empty();
                }
            })
            .collect(Collectors.toList());

        env.execute("TXT to CSV Converter using DataStream API");
        
        assertTrue(Files.list(dir.toPath()).anyMatch(Files::isRegularFile), "Output file was not created!");

        assertTrue(outputLines.contains("1,John,Doe,25"));
        assertTrue(outputLines.contains("2,Jane,Smith,45"));
    }
}
