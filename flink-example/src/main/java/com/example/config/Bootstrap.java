package com.example.config;

import org.apache.flink.configuration.MemorySize;

import java.time.Duration;


public class Bootstrap {
    public final String inputPath;
    public final String outputPath;
    public final String socketHostname;
    public final int socketPort;
    public final Duration rolloverInterval;
    public final Duration inactivityInterval;
    public final MemorySize maxPartSize;
    public final Duration boundedOutOfOrderness;
    public final Duration tumblingWindowSize;

    public Bootstrap() {
        Env env = new Env();
        inputPath = env.inputPath;
        outputPath = env.outPath;
        socketHostname = env.socketHostname;
        socketPort = env.socketPort;

        boundedOutOfOrderness = Duration.ofSeconds(Constants.boundedOutOfOrderness);
        tumblingWindowSize = Duration.ofSeconds(Constants.tumblingWindowSize);
        rolloverInterval = Duration.ofSeconds(Constants.rolloverInterval);
        maxPartSize = MemorySize.ofMebiBytes(Constants.maxPartSize);
        inactivityInterval = Duration.ofSeconds(Constants.inactivityInterval);
    }
    public Bootstrap(String path){
     FlinkProperties flinkProperties=new FlinkProperties(path);
     inputPath=flinkProperties.inputPath;
     outputPath=flinkProperties.outputPath;
     socketHostname=flinkProperties.socketHostname;
     socketPort=flinkProperties.socketPort;

     boundedOutOfOrderness=flinkProperties.boundedOutOfOrderness;
     tumblingWindowSize=flinkProperties.tumblingWindowSize;
     rolloverInterval=flinkProperties.rolloverInterval;
     maxPartSize=flinkProperties.maxPartSize;
     inactivityInterval=flinkProperties.inactivityInterval;
    }
}
