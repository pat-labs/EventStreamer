package com.example.config;

import org.apache.flink.configuration.MemorySize;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class FlinkProperties {
    public final String inputPath;
    public final String outputPath;
    public final String socketHostname;
    public final int socketPort;
    public final Duration rolloverInterval;
    public final Duration inactivityInterval;
    public final MemorySize maxPartSize;
    public final Duration boundedOutOfOrderness;
    public final Duration tumblingWindowSize;

    public FlinkProperties(String path) {
        Properties properties = new Properties();
        try (FileInputStream input = new FileInputStream(path)) {
            properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        inputPath = properties.getProperty("flink.input.path");
        outputPath = properties.getProperty("flink.output.path");
        socketHostname = properties.getProperty("flink.socket.hostname");
        socketPort = Integer.parseInt(properties.getProperty("flink.socket.port"));
        rolloverInterval=Duration.ofSeconds(Long.parseLong(properties.getProperty("flink.rollover.interval")));
        inactivityInterval=Duration.ofSeconds(Long.parseLong(properties.getProperty("flink.inactivity.interval")));
        maxPartSize = MemorySize.parse(properties.getProperty("flink.max.part.size"));
        boundedOutOfOrderness=Duration.ofSeconds(Long.parseLong(properties.getProperty("flink.bounded.out.orderness")));
        tumblingWindowSize=Duration.ofSeconds(Long.parseLong(properties.getProperty("flink.tumbling.window.size")));
    }
}