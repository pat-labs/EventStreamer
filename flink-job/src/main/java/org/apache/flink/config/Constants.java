package org.apache.flink.config;

public class Constants {
  public static final String dateTimeFormat = "yyyy MM.dd HH:mm:ss";
  public static final int numberSlotsPerTaskManager = 1;
  public static final int numberTaskManagers = 1;
  public static final int parallelism = 1;
  public static final int boundedOutOfOrderness = 10;
  public static final int tumblingWindowSize = 1;
  public static final int rolloverInterval = 3;
  public static final int maxPartSize = 1024;
  public static final int inactivityInterval = 3;
}
