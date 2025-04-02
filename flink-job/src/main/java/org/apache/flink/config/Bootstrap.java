package org.apache.flink.config;

import java.time.Duration;
import org.apache.flink.configuration.MemorySize;

public class Bootstrap {
  // Required
  public final boolean isProduction;
  public final String appPath;
  public final String inputPath;
  public final String outputPath;
  // Optional
  public final int numberSlotsPerTaskManager;
  public final int numberTaskManagers;
  public final int parallelism;
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

    this.numberSlotsPerTaskManager = builder.numberSlotsPerTaskManager;
    this.numberTaskManagers = builder.numberTaskManagers;
    this.parallelism = builder.parallelism;
    this.rolloverInterval = Duration.ofSeconds(builder.rolloverIntervalSeconds);
    this.inactivityInterval = Duration.ofSeconds(builder.inactivityIntervalSeconds);
    this.maxPartSize = MemorySize.ofMebiBytes(builder.maxPartSizeMegaBytes);
    this.boundedOutOfOrderness = Duration.ofSeconds(builder.boundedOutOfOrdernessSeconds);
    this.tumblingWindowSize = Duration.ofSeconds(builder.tumblingWindowSizeSeconds);
  }

  @Override
  public String toString() {
    return "Bootstrap{"
        + "isProduction="
        + isProduction
        + ", appPath='"
        + appPath
        + '\''
        + ", inputPath='"
        + inputPath
        + '\''
        + ", outputPath='"
        + outputPath
        + '\''
        + ", rolloverInterval="
        + rolloverInterval
        + ", inactivityInterval="
        + inactivityInterval
        + ", maxPartSize="
        + maxPartSize
        + ", boundedOutOfOrderness="
        + boundedOutOfOrderness
        + ", tumblingWindowSize="
        + tumblingWindowSize
        + '}';
  }

  public static class Builder {

    // Required
    public final boolean isProduction;
    private final String appPath;
    private final String inputPath;
    private final String outputPath;

    // Optional
    private int numberSlotsPerTaskManager = Constants.numberSlotsPerTaskManager;
    private int numberTaskManagers = Constants.numberTaskManagers;
    private int parallelism = Constants.parallelism;
    private long rolloverIntervalSeconds = Constants.rolloverInterval;
    private long inactivityIntervalSeconds = Constants.inactivityInterval;
    private long maxPartSizeMegaBytes = Constants.maxPartSize;
    private long boundedOutOfOrdernessSeconds = Constants.boundedOutOfOrderness;
    private long tumblingWindowSizeSeconds = Constants.tumblingWindowSize;

    public Builder(boolean isProduction, String appPath, String inputPath, String outputPath) {
      this.isProduction = isProduction;
      this.appPath = appPath;
      this.inputPath = inputPath;
      this.outputPath = outputPath;
    }

    public Builder numberSlotsPerTaskManager(int numberSlotsPerTaskManager) {
      this.numberSlotsPerTaskManager = numberSlotsPerTaskManager;
      return this;
    }

    public Builder numberTaskManagers(int numberTaskManagers) {
      this.numberTaskManagers = numberTaskManagers;
      return this;
    }

    public Builder parallelism(int parallelism) {
      this.parallelism = parallelism;
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
      this.maxPartSizeMegaBytes = maxPartSizeMebiBytes;
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
