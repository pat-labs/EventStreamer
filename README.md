# README

## Flink 1.20 Example Project with Java 17

This project demonstrates a basic Flink 1.20 application built with Java 17. It includes examples of data processing using Flink's DataStream API, specifically focusing on convertion from TXT to CSV data processing, windowing, and custom user-defined functions (UDFs). It provides instructions for running the application in both local Flink environments and Docker containers.

## Use Case

The job class `CsvProcessorStream` contains the main method `void process()` which receives the `Bootstrap` class as an argument. From this `Bootstrap` class, it retrieves the path to the data source (`input.txt`).

The operations within the job are as follows:

-   **File Reading:** Configures a data source (`FileSource`) to read text lines from a file specified by `inputFilePath`.
-   **Data Processing:** Converts the text lines into tuples (`Tuple4`) using `TransactionMapper` and filters out null tuples.
-   **Watermark Assignment:** Assigns watermarks and timestamps to the tuples to handle temporal disorder.
-   **Average Calculation:** Calculates averages over the processed tuples using a tumbling window.
-   **Result Writing:** Configures a sink (`FileSink`) to write the results to a CSV file in the path specified by `bootstrap.outputPath`, with file rotation policies.
-   **Error Handling:** Captures and logs any errors that occur during execution.

## Functions

-   **`AvgProcessWindowFunction`:** Defines a window function that calculates the average of a numeric field in a set of events grouped by a specific key, emitting the result in a new tuple.
-   **`SortProcessFunction`:** Defines a process function that configures a list state to store tuples, adds each tuple to the state, and registers a timer based on the event timestamp. When the timer is triggered, it sorts the stored tuples by the first field and emits them in order, then clears the state.

## Project Startup

The main class of the project is `Main.java`. From here, the project is started by calling the job. The configuration of the flink job is handled by the `Bootstrap.java` class, that uses the dotenv library to load the enviroment variables from the .env file. This class allows the user to configure the windows sizes, parallelism, and other flink configuration parameters.

**Key Features:**

* **CSV Data Processing:** Reads and processes CSV data from a file source.
* **Windowing:** Demonstrates tumbling window operations for time-based data aggregation.
* **Custom UDFs:** Includes custom UDFs for average calculation (`AvgProcessWindowFunction`) and sorting (`SortProcessFunction`).
* **Environment Configuration:** Uses `.env` files and the `dotenv-java` library for flexible configuration.
* **Docker Support:** Provides Docker Compose files for running Flink in both session and application modes.
* **Unit Testing:** Includes comprehensive unit tests for mappers and UDFs.

**Prerequisites:**

* [Java 17 or later](https://adoptium.net/es/temurin/releases/?os=windows&version=17&package=jdk)
* Maven
* [Docker (if running in Docker)](https://docs.docker.com/)
* [Flink 1.20 (if running locally)](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/configuration/overview/)

**Project Structure:**

```
Directory structure:
└── pat-labs-eventstreamer/
    ├── README.md
    ├── flink-application.yaml
    ├── flink-session.yaml
    ├── .env .dev
    ├── data/
    │   └── input.txt
    └── flink-example/
        ├── dependency-reduced-pom.xml
        ├── pom.xml
        ├── src/
            ├── main/
            │   └── java/
            │       └── com/
            │           └── example/
            │               ├── Main.java
            │               ├── config/
            │               │   ├── Bootstrap.java
            │               │   ├── Constants.java
            │               │   └── EnvLoader.java
            │               ├── job/
            │               │   └── CsvProcessorStream.java
            │               ├── mapper/
            │               │   └── TransactionMapper.java
            │               └── udf/
            │                   ├── AvgProcessWindowFunction.java
            │                   └── SortProcessFunction.java
            ├── resources/
            │   └── log4j2.properties
            └── test/
                └── java/
                    └── com/
                        └── example/
                            ├── mapper/
                            │   └── TransactionMapperTest.java
                            ├── udf/
                            │   ├── AverageProcessTest.java
                            │   └── SortProcessTest.java
                            └── utils/
                                └── CollectSink.java

```

**Before Execution:**

1.  **Data Directory Permissions (Linux):**
    * If you're running on Linux, ensure the `data` directory has the necessary permissions:

    ```bash
    chmod -R 777 data
    ```

2.  **Build the JAR:**
    * Use Maven to build the project and create the executable JAR file:

    ```bash
    mvn clean package
    ```

    * The JAR file will be located in the `target` directory (e.g., `target/flink-example-1.0-SNAPSHOT.jar`).

**Running the Application:**

**1. Running Locally with Flink:**

1.  **Start Flink:** Ensure your local Flink cluster is running.
2.  **Execute the JAR:** Open a console and run the following command, replacing `/flink-example/target/` with the correct path to your Flink user library directory:

    ```bash
    flink run /flink-example/target/flink-example-1.0-SNAPSHOT.jar
    ```

**2. Running with Docker:**

**2.1. Session Mode:**

1.  **Start the Flink Session Cluster:**

    ```bash
    docker compose -f flink-session.yaml up --scale taskmanager=2 -d
    ```

    * This command starts a Flink session cluster with two task managers.

2.  **Access the Flink JobManager Container:**

    ```bash
    docker exec -it <jobmanager_container_name> bash
    ```

    * Replace `<jobmanager_container_name>` with the actual name of your JobManager container.

3.  **Run the JAR:**

    ```bash
    flink run /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar
    ```

**2.2. Application Mode:**

1.  **Start the Flink Application Cluster:**

    ```bash
    docker compose -f flink-application.yaml up -d
    ```

    * **Important:** If your JAR file has a different name or path, you must modify the `flink-application.yaml` file in the `jobmanager` service configuration.

**Configuration:**

* **Log4j 2:** The project uses Log4j 2 for logging. You can configure logging behavior by modifying the `src/main/resources/log4j2.xml` file.
* **Environment Variables:** Environment variables can be configured using a `.env` file, and loaded using the dotenv library.
* **Docker Compose:** The `flink-session.yaml` and `flink-application.yaml` files define the Docker Compose configurations for running Flink in session and application modes, respectively.

**Testing:**

* The project includes unit tests for verifying the functionality of the Flink application. You can run the tests using Maven:

    ```bash
    mvn test
    ```

**Dependencies:**

* Apache Flink 1.20
* Log4j 2
* SLF4j
* [dotenv-java](https://github.com/cdimascio/dotenv-java)
* JUnit 5

**Notes:**

* Replace placeholder paths and names with your actual values.
* Refer to the official Apache Flink documentation for more detailed information on Flink concepts and configurations.
* Ensure that the java version used to compile the jar is the same as the java version used to run Flink.
* For production environments, consider using a more robust deployment strategy and monitoring tools.
