package com.example;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.FormatDescriptor;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.*;

public class CsvProcessorTable {
    public static void main(String[] args) throws Exception {
        String outputPath = "/opt/flink/data/output.csv";
        execTableEnv(outputPath);
    }

    private static void execTableEnv(String outputPath) throws Exception{
        EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .inStreamingMode()
            .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        final Schema personSchema = Schema.newBuilder()
            .column("person_id", DataTypes.STRING())
            .column("name", DataTypes.STRING()) 
            .column("last_name", DataTypes.STRING()) 
            .column("age", DataTypes.INT()) 
            .column("create_at", DataTypes.TIMESTAMP())
            .build();

        
        final TableDescriptor sourceDescriptor = TableDescriptor.forConnector("datagen")
                    .schema(personSchema)
                    .option("rows-per-second", "100")
                    .option("number-of-rows", "1000")
                    .build();
        tEnv.createTable("Person", sourceDescriptor);

        tEnv.createTemporaryTable("CsvSinkTable", TableDescriptor.forConnector("filesystem")
                .schema(personSchema)
                .option("path", outputPath)
                .format(FormatDescriptor.forFormat("csv")
                        .option("field-delimiter", "-")
                        .option("write-mode", "overwrite")
                        .build())
                .build());

        Table result = tEnv.sqlQuery(
            "SELECT person_id, name, last_name, age, create_at " +
                    "FROM Person " +
                    "WHERE age > 18 "
        );

        TableResult tableResult = result.executeInsert("CsvSinkTable");
        tableResult.print();
    }
}
