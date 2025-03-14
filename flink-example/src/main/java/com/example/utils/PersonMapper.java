package com.example.utils;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.example.domain.Person;

public class PersonMapper {
    public static void toCustomParserFileStream(String outputPath, DataStream<String> textStream, FileSink<String> sink) {
        DataStream<Tuple4<String, String, String, Integer>> dataStream = textStream
            .map(new TransactionParser())
            .filter(tuple -> tuple != null);

        dataStream.map(tuple -> tuple.f0 + "," + tuple.f1 + "," + tuple.f2 + "," + tuple.f3)
                 .sinkTo(sink);
    }

    public static void toFileStream(String outputPath, DataStream<String> textStream, FileSink<String> sink) {
        DataStream<Tuple4<String, String, String, String>> dataStream = textStream
            .map(line -> {
                String[] parts = line.split("-");
                return new Tuple4<>(
                    parts[0],
                    parts.length > 1 ? parts[1] : "",
                    parts.length > 2 ? parts[2] : "",
                    parts.length > 3 ? parts[3] : ""
                );
            }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.STRING));

        dataStream.map(tuple -> String.join(",", tuple.f0, tuple.f1, tuple.f2, tuple.f3))
                 .sinkTo(sink);
    }

    public static void toCustomFileStream(String outputPath, DataStream<String> textStream, FileSink<String> sink) {
        DataStream<Person> dataStream = textStream
            .map(line -> {
                String[] parts = line.split("-");
                if (parts.length == 4) {
                    try {
                        int personId = Integer.parseInt(parts[0].trim());
                        String name = parts[1].trim();
                        String lastName = parts[2].trim();
                        int age = Integer.parseInt(parts[3].trim());
                        return new Person(personId, name, lastName, age);
                    } catch (NumberFormatException e) {
                        return null;
                    }
                } else {
                    return null;
                }
            })
            .filter(person -> person != null)
            .returns(Types.POJO(Person.class));

        dataStream.map(Person::toString)
                 .sinkTo(sink);
    }
}
