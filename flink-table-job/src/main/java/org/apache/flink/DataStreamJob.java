package org.apache.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;


public class DataStreamJob {
  public static void main(String[] args) {
    try {
      // Default SQL file path
      String sqlFilePath = "/opt/flink/data/job.sql";

      // If a path is provided via args, use it instead
      if (args.length == 1 && args[0] != null && !args[0].trim().isEmpty()) {
        sqlFilePath = args[0];
      } else if (args.length > 1) {
        // Too many arguments provided
        System.err.println("Usage Error: Too many arguments.");
        System.err.println("Usage: flink run <your-jar-file> --sql-file <path-to-sql-file>");
        System.exit(1);
      }

      System.out.println("Using SQL file at: " + sqlFilePath);

      // Optionally print or log the file path for confirmation
      System.out.println("Using SQL file at: " + sqlFilePath);

      // Read SQL file content
      String sqlContent = readSQLFile(sqlFilePath);
      if (sqlContent.isEmpty()) {
        System.err.println("SQL file is empty: " + sqlFilePath);
        System.exit(1);
      }

      // Set up Flink Table Environment
      EnvironmentSettings settings = EnvironmentSettings.newInstance()
              .inStreamingMode()
              .build();
      TableEnvironment tableEnv = TableEnvironment.create(settings);

      // Execute SQL statements
      for (String statement : sqlContent.split(";")) {
        String trimmedStatement = statement.trim();
        if (!trimmedStatement.isEmpty()) {
          System.out.println("Executing: " + trimmedStatement);
          tableEnv.executeSql(statement);
        }
      }
    } catch (Exception e) {
      System.err.println("Error executing SQL job: " + e.getMessage());
      e.printStackTrace();
      System.exit(1);
    }
  }

  private static String readSQLFile(String filePath) throws IOException {
    Path path = Path.of(filePath);
    if (!Files.exists(path)) {
      throw new IOException("SQL file not found: " + filePath);
    }
    List<String> lines = Files.readAllLines(path);
    return lines.stream().collect(Collectors.joining(System.lineSeparator()));
  }
}
