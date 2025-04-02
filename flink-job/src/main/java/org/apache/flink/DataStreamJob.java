package org.apache.flink;

import java.time.Duration;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.config.Bootstrap;
import org.apache.flink.config.EnvLoader;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.mapper.CustomerAccountMapper;
import org.apache.flink.mapper.ReportMapper;
import org.apache.flink.mapper.TransactionMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.udf.MaxPageWindowFunction;
import org.apache.flink.udf.PaginatedWindowFunction;
import org.apache.flink.udf.SortProcessFunction;
import org.apache.flink.udf.SumWindowFunction;
import org.apache.flink.util.Collector;

/*
# Processing Steps (Recipe)

1. **Initialize Environment**:
   - Load environment variables and create the bootstrap configuration.
   - Initialize the Flink streaming execution environment.

2. **Load and Parse Customer Data**:
   - Read customer data from a file.
   - Parse records into a structured format.
   - Filter out customers with missing or empty email addresses.

3. **Load and Parse Transaction Data**:
   - Read transaction data from a file.
   - Parse records into a structured format.

4. **Join Customers with Transactions**:
   - Perform an interval join between customers and transactions based on account ID.
   - Remove customers who do not have transactions.

5. **Sort Transactions for Each Customer**:
   - Sort transaction details per customer.

6. **Compute Total Transaction Amount per Customer**:
   - Aggregate the total transaction amount per customer using a tumbling event-time window.

7. **Paginate Transactions**:
   - Divide transactions into pages (20 records per page).
   - Determine the maximum number of pages per customer.

8. **Generate Summary Report**:
   - Join the computed total transaction amount with the maximum number of pages.
   - Output summary data per customer.

9. **Join Detailed Transactions with Pagination Info**:
   - Assign a page number to each transaction record.

10. **Join Detailed Transactions with Total Amount**:
	- Combine transactions with the total transaction sum per customer.

11. **Generate Final Report**:
	- Format and output the final report containing:
	  - Customer account ID
	  - Transaction date
	  - Page number
	  - Transaction description
	  - Transaction amount
	  - Total transaction sum per customer

12. **Execute the Flink Job**:
	- Trigger execution of the Flink job to process the pipeline.
*/

public class DataStreamJob {

  public static void main(String[] args) throws Exception {
    Bootstrap bootstrap = new EnvLoader("././.env").buildBootstrap();
    System.out.println("The current configuration is \n" + bootstrap.toString());

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(bootstrap.parallelism);

    final FileSink<String> sink =
        FileSink.forRowFormat(
                new Path(bootstrap.outputPath), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(bootstrap.rolloverInterval)
                    .withInactivityInterval(bootstrap.inactivityInterval)
                    .withMaxPartSize(bootstrap.maxPartSize)
                    .build())
            .build();

    String customerPath = bootstrap.inputPath + "/customers_output.txt";
    System.out.println("Processing file: " + customerPath);
    FileSource<String> customerSource =
        FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(customerPath)).build();
    DataStream<String> customerData =
        env.fromSource(customerSource, IngestionTimeWatermarkStrategy.create(), "source-account");

    DataStream<Tuple3<String, String, String>> customerStream =
        customerData
            .map(new CustomerAccountMapper())
            .filter(tuple -> tuple != null)
            .filter(tuple -> tuple.f2 != null && !tuple.f2.isEmpty());

    String transactionPath = bootstrap.inputPath + "/trx_output.txt";
    System.out.println("Processing file: " + transactionPath);
    // Files.lines(Paths.get(transactionPath)).forEach(System.out::println);
    FileSource<String> transactionSource =
        FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path(transactionPath))
            .build();
    DataStream<String> transactionData =
        env.fromSource(
            transactionSource, IngestionTimeWatermarkStrategy.create(), "source-account");

    DataStream<Tuple4<String, Long, String, Double>> transactionStream =
        transactionData.map(new TransactionMapper()).filter(tuple -> tuple != null);

    SingleOutputStreamOperator<Tuple4<String, Long, String, Double>> detailProcess =
        customerStream
            .keyBy(detail -> detail.f0)
            .intervalJoin(transactionStream.keyBy(customer -> customer.f0))
            .between(Duration.ofSeconds(-60), Duration.ofSeconds(60))
            .process(
                new ProcessJoinFunction<
                    Tuple3<String, String, String>,
                    Tuple4<String, Long, String, Double>,
                    Tuple4<String, Long, String, Double>>() {

                  @Override
                  public void processElement(
                      Tuple3<String, String, String> customer,
                      Tuple4<String, Long, String, Double> transaction,
                      Context ctx,
                      Collector<Tuple4<String, Long, String, Double>> out)
                      throws Exception {

                    out.collect(
                        new Tuple4<>(
                            transaction.f0, // codeAcc
                            transaction.f1, // dateMov
                            transaction.f2, // descMov
                            transaction.f3 // amount
                            ));
                  }
                });

    DataStream<Tuple4<String, Long, String, Double>> detailProcessSort =
        detailProcess.keyBy(tuple -> tuple.f0).process(new SortProcessFunction());

    DataStream<Tuple2<String, Double>> detailProcessSortSum =
        detailProcessSort
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(bootstrap.tumblingWindowSize))
            .process(new SumWindowFunction());

    DataStream<Tuple3<String, Long, Integer>> detailProcessSortPage =
        detailProcessSort
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(bootstrap.tumblingWindowSize))
            .process(new PaginatedWindowFunction(20));

    DataStream<Tuple2<String, Integer>> detailProcessSortPageMax =
        detailProcessSortPage
            .keyBy(tuple -> tuple.f0)
            .window(TumblingEventTimeWindows.of(bootstrap.tumblingWindowSize))
            .process(new MaxPageWindowFunction());

    SingleOutputStreamOperator<Tuple3<String, Integer, Double>> summaryReportProcess =
        detailProcessSortPageMax
            .keyBy(page -> page.f0)
            .intervalJoin(detailProcessSortSum.keyBy(sum -> sum.f0))
            .between(Duration.ofSeconds(-60), Duration.ofSeconds(60))
            .process(
                new ProcessJoinFunction<
                    Tuple2<String, Integer>,
                    Tuple2<String, Double>,
                    Tuple3<String, Integer, Double>>() {

                  @Override
                  public void processElement(
                      Tuple2<String, Integer> page,
                      Tuple2<String, Double> sum,
                      Context ctx,
                      Collector<Tuple3<String, Integer, Double>> out)
                      throws Exception {

                    out.collect(
                        new Tuple3<>(
                            page.f0, // codeAcc
                            page.f1, // maxPage
                            sum.f1 // totalSum
                            ));
                  }
                });
    System.out.println("Summary");
    summaryReportProcess
        // Customer: {codeAcc}, Total Pages: {page}, Total Amount: {sum}
        .map(
            tuple ->
                "Customer: "
                    + tuple.f0
                    + ", Total Pages: "
                    + tuple.f1
                    + ", Total Amount: "
                    + tuple.f2)
        .sinkTo(sink);

    SingleOutputStreamOperator<Tuple5<String, Long, Integer, String, Double>> reportProcessPage =
        detailProcessSort
            .keyBy(detailSort -> detailSort.f0 + detailSort.f1)
            .intervalJoin(detailProcessSortPage.keyBy(page -> page.f0 + page.f1))
            .between(Duration.ofSeconds(-60), Duration.ofSeconds(60))
            .process(
                new ProcessJoinFunction<
                    Tuple4<String, Long, String, Double>,
                    Tuple3<String, Long, Integer>,
                    Tuple5<String, Long, Integer, String, Double>>() {

                  @Override
                  public void processElement(
                      Tuple4<String, Long, String, Double> detailSort,
                      Tuple3<String, Long, Integer> page,
                      Context ctx,
                      Collector<Tuple5<String, Long, Integer, String, Double>> out)
                      throws Exception {

                    out.collect(
                        new Tuple5<>(
                            detailSort.f0, // codeAcc
                            detailSort.f1, // dateMov
                            page.f2, // page
                            detailSort.f2, // descMov
                            detailSort.f3 // amount
                            ));
                  }
                });

    SingleOutputStreamOperator<Tuple6<String, Long, Integer, String, Double, Double>>
        reportProcessPageSum =
            reportProcessPage
                .keyBy(detailPage -> detailPage.f0)
                .intervalJoin(detailProcessSortSum.keyBy(sum -> sum.f0))
                .between(Duration.ofSeconds(-60), Duration.ofSeconds(60))
                .process(
                    new ProcessJoinFunction<
                        Tuple5<String, Long, Integer, String, Double>,
                        Tuple2<String, Double>,
                        Tuple6<String, Long, Integer, String, Double, Double>>() {

                      @Override
                      public void processElement(
                          Tuple5<String, Long, Integer, String, Double> detailPage,
                          Tuple2<String, Double> sum,
                          Context ctx,
                          Collector<Tuple6<String, Long, Integer, String, Double, Double>> out)
                          throws Exception {

                        out.collect(
                            new Tuple6<>(
                                detailPage.f0, // codeAcc
                                detailPage.f1, // dateMov
                                detailPage.f2, // page
                                detailPage.f3, // desMov
                                detailPage.f4, // amount
                                sum.f1 // sum
                                ));
                      }
                    });
    System.out.println("Report");
    reportProcessPageSum
        .keyBy(value -> value.f0)
        .map(new ReportMapper())
        // .map(tuple -> tuple.f0 + "," + tuple.f1 + "," + tuple.f2 + "," + tuple.f3 + "," +
        // tuple.f4 + "," + tuple.f5)
        .sinkTo(sink);
    // Execute program, beginning computation.
    env.execute("Flink Java API Skeleton");
  }
}
