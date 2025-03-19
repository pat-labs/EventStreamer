## Flink 1.20 Example Project with Java 17

This project demonstrates a basic Flink 1.20 application built with Java 17. It includes examples of data processing using Flink's DataStream API and provides instructions for running the application in both local Flink environments and Docker containers.

**Prerequisites:**

* Java 17 or later
* Maven
* Docker (if running in Docker)
* Flink 1.20 (if running locally)

**Project Structure:**

```
flink-example/
├── src/
│   ├── main/
│   │   ├── java/
│   │   │   └── com/example/
│   │   │       └── ... (Your Java source files)
│   │   └── resources/
│   │       └── log4j2.xml (Log4j 2 configuration)
├── data/ (Input and output data directory)
├── flink-session.yaml (Docker Compose for Flink session mode)
├── flink-application.yaml (Docker Compose for Flink application mode)
├── pom.xml (Maven project configuration)
└── README.md
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
2.  **Execute the JAR:** Open a console and run the following command, replacing `/opt/flink/usrlib/` with the correct path to your Flink user library directory:

    ```bash
    flink run /opt/flink/usrlib/flink-example-1.0-SNAPSHOT.jar
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
* dotenv-java
* JUnit 5

**Notes:**

* Replace placeholder paths and names with your actual values.
* Refer to the official Apache Flink documentation for more detailed information on Flink concepts and configurations.
* Ensure that the java version used to compile the jar is the same as the java version used to run Flink.
* For production environments, consider using a more robust deployment strategy and monitoring tools.
