package com.example;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.NetUtils;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class SocketToCsvTest extends AbstractTestBase {

    @Parameterized.Parameter 
    public boolean asyncState;

    @Parameterized.Parameters
    public static Collection<Boolean> setup() {
        return Arrays.asList(false, true);
    }

    @Test
    public void testJavaProgram() throws Exception {
        String socketHost = System.getenv().getOrDefault("SOCKET_HOST", "localhost");
        InetAddress hostAddress = InetAddress.getByName(socketHost);

        final PrintStream originalSysout = System.out;
        final PrintStream originalSyserr = System.err;
        final ByteArrayOutputStream errorMessages = new ByteArrayOutputStream();

        System.setOut(new PrintStream(new NullStream()));
        System.setErr(new PrintStream(errorMessages));

        try {
            try (ServerSocket server = new ServerSocket(0, 10, hostAddress)) {

                final ServerThread serverThread = new ServerThread(server);
                serverThread.setDaemon(true);
                serverThread.start();

                final int serverPort = server.getLocalPort();
                System.out.println("Server listening on " + socketHost + ":" + serverPort);

                SocketToCsv.main(new String[]{"--hostname", socketHost, "--port", String.valueOf(serverPort)});

                if (errorMessages.size() != 0) {
                    fail("Found error message: " +
                         new String(errorMessages.toByteArray(), ConfigConstants.DEFAULT_CHARSET));
                }

                serverThread.join();
                serverThread.checkError();

                validateCsvOutput();
            }
        } finally {
            System.setOut(originalSysout);
            System.setErr(originalSyserr);
        }
    }

    private void validateCsvOutput() throws IOException {
        //String outputPath = "/opt/flink/data/output.csv";
        String outputPath = "/data/output.csv";
        Path output = Paths.get(outputPath);
        assertTrue(Files.exists(output), "Output file does not exist");

        List<String> lines = Files.readAllLines(output);
        assertFalse(lines.isEmpty(), "Output file is empty");

        System.out.println("CSV Output Validation Passed ✅");
    }

    // ------------------------------------------------------------------------

    private static class ServerThread extends Thread {

        private final ServerSocket serverSocket;
        private volatile Throwable error;

        public ServerThread(ServerSocket serverSocket) {
            super("Socket Server Thread");
            this.serverSocket = serverSocket;
        }

        @Override
        public void run() {
            try {
                try (Socket socket = NetUtils.acceptWithoutTimeout(serverSocket);
                     PrintWriter writer = new PrintWriter(socket.getOutputStream(), true)) {

                    writer.println("hello world flink flink");
                }
            } catch (Throwable t) {
                this.error = t;
            }
        }

        public void checkError() throws IOException {
            if (error != null) {
                throw new IOException("Error in server thread: " + error.getMessage(), error);
            }
        }
    }

    private static final class NullStream extends OutputStream {
        @Override
        public void write(int b) {}
    }
}

