package org.apache.flink;

import org.apache.flink.api.common.eventtime.*;

public class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {

  private IngestionTimeWatermarkStrategy() {}

  public static <T> IngestionTimeWatermarkStrategy<T> create() {
    return new IngestionTimeWatermarkStrategy<>();
  }

  @Override
  public WatermarkGenerator<T> createWatermarkGenerator(
      WatermarkGeneratorSupplier.Context context) {
    return new AscendingTimestampsWatermarks<>();
  }

  @Override
  public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
    return (event, timestamp) -> System.currentTimeMillis();
  }
}
