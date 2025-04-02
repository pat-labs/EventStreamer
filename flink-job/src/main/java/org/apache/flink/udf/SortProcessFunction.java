package org.apache.flink.udf;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class SortProcessFunction
    extends KeyedProcessFunction<
        String, Tuple4<String, Long, String, Double>, Tuple4<String, Long, String, Double>> {

  private transient ListState<Tuple4<String, Long, String, Double>> bufferState;

  @Override
  public void open(OpenContext openContext) throws Exception {
    ListStateDescriptor<Tuple4<String, Long, String, Double>> descriptor =
        new ListStateDescriptor<>(
            "bufferState", Types.TUPLE(Types.STRING, Types.LONG, Types.STRING, Types.DOUBLE));
    bufferState = getRuntimeContext().getListState(descriptor);
  }

  @Override
  public void processElement(
      Tuple4<String, Long, String, Double> value,
      Context ctx,
      Collector<Tuple4<String, Long, String, Double>> out)
      throws Exception {

    bufferState.add(value);

    long eventTime = value.f1;
    long triggerTime = eventTime + 1;

    ctx.timerService().registerEventTimeTimer(triggerTime); // Register the event time timer
  }

  @Override
  public void onTimer(
      long timestamp, OnTimerContext ctx, Collector<Tuple4<String, Long, String, Double>> out)
      throws Exception {
    List<Tuple4<String, Long, String, Double>> sortedList = new ArrayList<>();
    for (Tuple4<String, Long, String, Double> item : bufferState.get()) {
      sortedList.add(item);
    }

    sortedList.sort(Comparator.comparing(t -> t.f1));

    for (Tuple4<String, Long, String, Double> item : sortedList) {
      out.collect(item);
    }

    bufferState.clear();
  }
}
