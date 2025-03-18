package com.example.config;

import org.apache.flink.configuration.MemorySize;

import java.time.Duration;


public class Bootstrap {
    // Required
    public final boolean isProduction;
    public final String appPath;
    public final String inputPath;
    public final String outputPath;
    // Optional
    public final String socketHostname;
    public final int socketPort;
    public final Duration rolloverInterval;
    public final Duration inactivityInterval;
    public final MemorySize maxPartSize;
    public final Duration boundedOutOfOrderness;
    public final Duration tumblingWindowSize;

    public Bootstrap(Builder builder) {
        this.isProduction = builder.isProduction;
        this.appPath = builder.appPath;
        this.inputPath = builder.inputPath;
        this.outputPath = builder.outputPath;
        this.socketHostname = builder.socketHostname;
        this.socketPort = builder.socketPort;
        this.rolloverInterval = Duration.ofSeconds(builder.rolloverIntervalSeconds);
        this.inactivityInterval = Duration.ofSeconds(builder.inactivityIntervalSeconds);
        this.maxPartSize = MemorySize.ofMebiBytes(builder.maxPartSizeMebiBytes);
        this.boundedOutOfOrderness = Duration.ofSeconds(builder.boundedOutOfOrdernessSeconds);
        this.tumblingWindowSize = Duration.ofSeconds(builder.tumblingWindowSizeSeconds);
    }

    public static class Builder {

        // Required
        public final boolean isProduction;
        private final String appPath;
        private final String inputPath;
        private final String outputPath;

        // Optional
        private String socketHostname = null;
        private int socketPort = 0;
        private long rolloverIntervalSeconds = Constants.rolloverInterval;
        private long inactivityIntervalSeconds = Constants.inactivityInterval;
        private long maxPartSizeMebiBytes = Constants.maxPartSize;
        private long boundedOutOfOrdernessSeconds = Constants.boundedOutOfOrderness;
        private long tumblingWindowSizeSeconds = Constants.tumblingWindowSize;

        public Builder(boolean isProduction, String appPath, String inputPath, String outputPath) {
            this.isProduction = isProduction;
            this.appPath = appPath;
            this.inputPath = inputPath;
            this.outputPath = outputPath;
        }

        public Builder socketHostname(String socketHostname) {
            this.socketHostname = socketHostname;
            return this;
        }

        public Builder socketPort(int socketPort) {
            this.socketPort = socketPort;
            return this;
        }

        public Builder rolloverInterval(long rolloverIntervalSeconds) {
            this.rolloverIntervalSeconds = rolloverIntervalSeconds;
            return this;
        }

        public Builder inactivityInterval(long inactivityIntervalSeconds) {
            this.inactivityIntervalSeconds = inactivityIntervalSeconds;
            return this;
        }

        public Builder maxPartSize(long maxPartSizeMebiBytes) {
            this.maxPartSizeMebiBytes = maxPartSizeMebiBytes;
            return this;
        }

        public Builder boundedOutOfOrderness(long boundedOutOfOrdernessSeconds) {
            this.boundedOutOfOrdernessSeconds = boundedOutOfOrdernessSeconds;
            return this;
        }

        public Builder tumblingWindowSize(long tumblingWindowSizeSeconds) {
            this.tumblingWindowSizeSeconds = tumblingWindowSizeSeconds;
            return this;
        }

        public Bootstrap build() {
            return new Bootstrap(this);
        }
    }
}
