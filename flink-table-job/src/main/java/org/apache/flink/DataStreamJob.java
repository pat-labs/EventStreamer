package org.apache.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.ParameterTool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;


public class DataStreamJob {
  public static void main(String[] args) {
    try {
      // Parse command-line arguments
      ParameterTool parameterTool = ParameterTool.fromArgs(args);
      String sqlFilePath = parameterTool.get("sql-file");

      if (sqlFilePath == null || sqlFilePath.isEmpty()) {
        System.err.println("Usage Error: flink run /opt/flink/usrlib/flink-table-job-1.0.0.jar --sql-file /opt/flink/data/job.sql");
        System.exit(1);
      }

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
