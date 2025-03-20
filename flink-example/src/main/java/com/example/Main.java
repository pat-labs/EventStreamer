package com.example;

import com.example.config.Bootstrap;
import com.example.config.EnvLoader;
import com.example.job.TransactionReport;

public class Main {
    public static void main(String[] args) throws Exception {
        // Setup the variable of the project
        Bootstrap bootstrap = new EnvLoader("././.env").buildBootstrap();
        System.out.println("The current configuration is \n" + bootstrap.toString());
        // Change the Job that you want to Excecute 
        // Note*: the folder test refers to this file so if you want to delete, alter the text to use
        try{
            TransactionReport.process(bootstrap);
        } catch (Throwable t) {
            System.out.println("ERROR: " + t);
        }
    }
    
}
