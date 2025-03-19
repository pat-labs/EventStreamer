package com.example.config;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.function.Predicate;

public class EnvLoader {
    private final Dotenv dotenv;
    public final String appPath;
    public final boolean isProdcution;
    public final String inputPath;
    public final String outputPath;
    public final int numberTaskManagers;
    public final int parallelism;

    public EnvLoader(String envFilePath) throws NumberFormatException{
        dotenv = Dotenv.configure()
        .directory(envFilePath)
        .ignoreIfMalformed()
        .ignoreIfMissing()
        .load();
        appPath = dotenv.get("APP_PATH", "flink-example/src");
        inputPath = dotenv.get("INPUT_PATH", "/data");
        outputPath = dotenv.get("OUTPUT_PATH", "/data/output");

        String isProductionStr = dotenv.get("IS_PRODUCTION", "0");
        Predicate<String> isProduction = s -> "1".equals(s);
        isProdcution = isProduction.test(isProductionStr);

        numberTaskManagers = Integer.parseInt(dotenv.get("NUMBER_TASK_MANAGERS", "2"));
        parallelism = Integer.parseInt(dotenv.get("PARALLELISM", "1"));
    }

    public Bootstrap buildBootstrap(){
        return new Bootstrap.Builder(isProdcution, appPath, inputPath, outputPath).parallelism(parallelism).build();
    }
}