package com.example.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Predicate;

public class PropertiesLoader {
    private final Properties properties;
    public final String appPath;
    public final boolean isProdcution;
    public final String inputPath;
    public final String outPath;
    public final String socketHostname;
    public final int socketPort;

    public PropertiesLoader(String propertiesFilePath) {
        properties = new Properties();
        try (FileInputStream input = new FileInputStream(propertiesFilePath)) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file: " + propertiesFilePath, e);
        }

        appPath = properties.getProperty("app.path", "flink-example/src");
        inputPath = properties.getProperty("app.input.path", "/data");
        outPath = properties.getProperty("app.output.path", "/data/output");
        socketHostname = properties.getProperty("app.socket.hostname", "localhost");

        String isProductionStr = properties.getProperty("app.is_production", "0");
        Predicate<String> isProduction = s -> "1".equals(s);
        isProdcution = isProduction.test(isProductionStr);
        
        String socketPortStr = properties.getProperty("app.socket.port", "9000");
        int port;
        try {
            port = Integer.parseInt(socketPortStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid socket port: " + socketPortStr, e);
        }
        socketPort = port;
    }

    public Bootstrap buildBootstrap() {
        return new Bootstrap.Builder(isProdcution, appPath, inputPath, outPath)
                .socketHostname(socketHostname)
                .socketPort(socketPort)
                .build();
    }
}

