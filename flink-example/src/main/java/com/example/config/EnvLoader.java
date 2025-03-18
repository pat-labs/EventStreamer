package com.example.config;

import io.github.cdimascio.dotenv.Dotenv;
import java.util.function.Predicate;

public class EnvLoader {
    private final Dotenv dotenv;
    public final String appPath;
    public final boolean isProdcution;
    public final String inputPath;
    public final String outPath;
    public final String socketHostname;
    public final int socketPort;

    public EnvLoader() {
        dotenv = Dotenv.configure()
        .directory("././.env")
        .ignoreIfMalformed()
        .ignoreIfMissing()
        .load();
        appPath = dotenv.get("APP_PATH", "flink-example/src");
        inputPath = dotenv.get("INPUT_PATH", "/data");
        outPath = dotenv.get("OTUPATH", "/data/output");
        socketHostname = dotenv.get("SOCKET_HOSTNAME", "localhost");

        String isProductionStr = dotenv.get("IS_PRODUCTION", "0");
        Predicate<String> isProduction = s -> "1".equals(s);
        isProdcution = isProduction.test(isProductionStr);

        String socketPortStr = dotenv.get("SOCKET_PORT", "9000");
        int port;
        try {
            port = Integer.parseInt(socketPortStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid socket port: " + socketPortStr, e);
        }
        socketPort = port;
    }

    public Bootstrap buildBootstrap(){
        return new Bootstrap.Builder(isProdcution, appPath, inputPath, outPath)
                .socketHostname(socketHostname)
                .socketPort(socketPort)
                .build();
    }
}