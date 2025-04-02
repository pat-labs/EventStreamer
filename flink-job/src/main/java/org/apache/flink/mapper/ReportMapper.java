package org.apache.flink.mapper;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple6;

public class ReportMapper
    extends RichMapFunction<Tuple6<String, Long, Integer, String, Double, Double>, String> {
  private transient MapState<String, String> lastHeader;
  private transient MapState<String, StringBuilder> detailBuffers;

  @Override
  public void open(OpenContext ctx) throws Exception {
    MapStateDescriptor<String, String> lastHeaderDesc =
        new MapStateDescriptor<>("lastHeader", String.class, String.class);
    lastHeader = getRuntimeContext().getMapState(lastHeaderDesc);

    MapStateDescriptor<String, StringBuilder> detailBuffersDesc =
        new MapStateDescriptor<>("detailBuffers", String.class, StringBuilder.class);
    detailBuffers = getRuntimeContext().getMapState(detailBuffersDesc);
  }

  @Override
  public String map(Tuple6<String, Long, Integer, String, Double, Double> value) throws Exception {
    String key = value.f0 + "-" + value.f2;
    String currentHeader =
        "---------------------------------------\n"
            + "customer: "
            + value.f0
            + " total amount: "
            + value.f5
            + "\n"
            + "Page: "
            + value.f2
            + "\n"
            + "---------------------------------------\n";
    String detail = value.f3 + " " + value.f4;

    if (detailBuffers.get(key) == null) {
      detailBuffers.put(key, new StringBuilder());
    }
    detailBuffers.get(key).append(detail);

    if (lastHeader.get(key) != null && lastHeader.get(key).equals(currentHeader)) {
      String result = detailBuffers.get(key).toString();
      detailBuffers.remove(key);
      return result;
    } else {
      String result = currentHeader + detailBuffers.get(key).toString();
      lastHeader.put(key, currentHeader);
      detailBuffers.remove(key);
      return result;
    }
  }
}
