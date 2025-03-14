package com.example.config;

import java.time.Duration;

import io.github.cdimascio.dotenv.Dotenv;

public class Env {
    private final Dotenv dotenv;
    public final Duration boundedOutOfOrderness;
    public final Duration tumblingWindowSize;

    public Env() {
        dotenv = Dotenv.load();
        boundedOutOfOrderness = Duration.ofSeconds(Integer.parseInt(dotenv.get("BOUNDED_OUT_OF_ORDERNESS", "6")));
        tumblingWindowSize = Duration.ofSeconds(Integer.parseInt(dotenv.get("TUMBLING_WINDOW_SIZE", "5")));
    }
}