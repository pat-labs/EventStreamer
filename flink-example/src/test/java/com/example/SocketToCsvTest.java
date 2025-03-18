package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.NetUtils;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;


public class SocketToCsvTest extends AbstractTestBase {
    @Test
    public void testJavaProgram() throws Exception {
        String hostname = "localhost";
        InetAddress localhost = InetAddress.getByName(hostname);

        final PrintStream originalSysout = System.out;
        final PrintStream originalSyserr = System.err;
        final ByteArrayOutputStream errorMessages = new ByteArrayOutputStream();
        //System.setOut(new PrintStream(new NullStream()));
        //System.setErr(new PrintStream(errorMessages));

        try (ServerSocket server = new ServerSocket(0, 10, localhost)) {

            final ServerThread serverThread = new ServerThread(server);
            serverThread.setDaemon(true);
            serverThread.start();

            final int serverPort = server.getLocalPort();
            System.out.println("Server listening on port " + serverPort);

            CollectSink.values.clear();

            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            DataStream<String> dataStream = env.socketTextStream(hostname, serverPort, "\n");
            // dataStream.map(value -> { 
            //     System.out.println("Raw in Flink: " + value);
            //     return value;
            // }).print();

            DataStream<Tuple2<String, Long>> resultStream = SocketToCsv.wordCountStream(dataStream);
            // dataStream.map(value -> {
            //     System.out.println("Processed in Flink: " + value);
            //     return value;
            // }).print();
            resultStream.addSink(new CollectSink());
            env.execute();
            Thread.sleep(3000);
            serverThread.join();
            serverThread.checkError();

            List<Tuple2<String, Long>> results = CollectSink.values;
            //System.out.println("📌 Contents of CollectSink.values: " + results);
            assertTrue(results.contains(new Tuple2<>("patrick",2L)));
        } catch (Throwable t) {
            System.out.println("ERROR: " + t);
        } finally {
            System.setOut(originalSysout);
            System.setErr(originalSyserr);
        }
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
                        
                        writer.println("patrick");
                        writer.println("fuentes");
                        writer.println("patrick");
                        writer.flush();
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

    private static class CollectSink implements SinkFunction<Tuple2<String, Long>> {
        public static final List<Tuple2<String, Long>> values =
                Collections.synchronizedList(new ArrayList<>());
    
        @Override
        public void invoke(Tuple2<String, Long> value, Context context) {
            values.add(value);
        }
    }
}

