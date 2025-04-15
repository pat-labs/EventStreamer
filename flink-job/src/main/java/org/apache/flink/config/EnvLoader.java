package org.apache.flink.config;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.function.Predicate;

public class EnvLoader {
  private final Dotenv dotenv;
  public final String appPath;
  public final boolean isProduction;
  public final String inputPath;
  public final String outputPath;
  public final int numberTaskManagers;
  public final int parallelism;

  public EnvLoader(String envFilePath) throws NumberFormatException {
    dotenv = Dotenv.configure().directory(envFilePath).ignoreIfMalformed().ignoreIfMissing().load();
    appPath = dotenv.get("APP_PATH", "/opt/flink/usrlib/flink-job/src");
    inputPath = dotenv.get("INPUT_PATH", "/opt/flink/data");
    outputPath = dotenv.get("OUTPUT_PATH", "/opt/flink/data/output");

    String isProductionStr = dotenv.get("IS_PRODUCTION", "0");
    Predicate<String> isProduction = s -> "1".equals(s);
    this.isProduction = isProduction.test(isProductionStr);

    numberTaskManagers = Integer.parseInt(dotenv.get("NUMBER_TASK_MANAGERS", "2"));
    parallelism = Integer.parseInt(dotenv.get("PARALLELISM", "1"));
  }

  public Bootstrap buildBootstrap() {
    return new Bootstrap.Builder(isProduction, appPath, inputPath, outputPath)
        .parallelism(parallelism)
        .build();
  }
}
