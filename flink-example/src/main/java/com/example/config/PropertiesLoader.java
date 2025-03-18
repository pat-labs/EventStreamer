package com.example.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesLoader {
    public final String appPath;
    public final boolean isProduction;
    public final String inputPath;
    public final String outPath;
    public final String socketHostname;
    public final int socketPort;

    public static PropertiesLoader fromFile() {
        Properties properties = new Properties();
        String propertiesFilePath = "././app.properties";
        try (FileInputStream input = new FileInputStream(propertiesFilePath)) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Error loading properties file: " + propertiesFilePath, e);
        }
        return new PropertiesLoader(properties);
    }

    public PropertiesLoader(Properties properties) {
        this.appPath = properties.getProperty("app.path", "flink-example/src");
        this.inputPath = properties.getProperty("app.input.path", "/data");
        this.outPath = properties.getProperty("app.output.path", "/data/output");
        this.socketHostname = properties.getProperty("app.socket.hostname", "localhost");

        // Conversión de string a boolean
        String isProductionStr = properties.getProperty("app.is_production", "0");
        this.isProduction = "1".equals(isProductionStr);

        // Conversión de puerto a entero con manejo de errores
        String socketPortStr = properties.getProperty("app.socket.port", "9000");
        int port;
        try {
            port = Integer.parseInt(socketPortStr);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid socket port: " + socketPortStr, e);
        }
        this.socketPort = port;
    }

    public Bootstrap buildBootstrap() {
        return new Bootstrap.Builder(isProduction, appPath, inputPath, outPath)
                .socketHostname(socketHostname)
                .socketPort(socketPort)
                .build();
    }
}
