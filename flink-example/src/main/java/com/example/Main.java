package com.example;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.config.Bootstrap;
import com.example.config.EnvLoader;
import com.example.job.CsvProcessorStream;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    public static void main(String[] args) throws Exception {
        // Setup the variable of the project
        Bootstrap bootstrap = new EnvLoader("././.env").buildBootstrap();
        logger.info("The current configuration is \n" + bootstrap.toString());
        // Change the Job that you want to Excecute 
        // Note*: the folder test refers to this file so if you want to delete, alter the text to use
        CsvProcessorStream.process(bootstrap);
    }
    
}
