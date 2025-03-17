package com.example.config;

import io.github.cdimascio.dotenv.Dotenv;

public class Env {
    private final Dotenv dotenv;
    public final String inputPath;
    public final String outPath;
    public final String socketHostname;
    public final int socketPort;

    public Env() {
        dotenv = Dotenv.load();
        inputPath = dotenv.get("INPUT_PATH", "/data");
        outPath = dotenv.get("OTUPATH", "/data/output");
        socketHostname = dotenv.get("SOCKET_HOSTNAME", "localhost");
        socketPort = Integer.parseInt(dotenv.get("SOCKET_PORT", "9000"));
    }
}