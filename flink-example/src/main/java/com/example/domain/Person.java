package com.example.domain;

public class Person {
    public int personId;
    public String name;
    public String lastName;
    public int age;

    @SuppressWarnings("unused")
    public Person() {}

    public Person(int personId, String name, String lastName, int age) {
        this.personId = personId;
        this.name = name;
        this.lastName = lastName;
        this.age = age;
    }

    @Override
    public String toString() {
        return personId + "," + name + "," + lastName + "," + age;
    }
}
