package com.example.config;
import org.apache.flink.configuration.MemorySize;

import java.time.Duration;

public class Constants {
    public static final Duration ROLLOVER_INTERVAL = Duration.ofSeconds(3);
    public static final MemorySize MAX_PART_SIZE = MemorySize.ofMebiBytes(1024);
    public static final Duration INACTIVITY_INTERVAL = Duration.ofSeconds(10);
}
