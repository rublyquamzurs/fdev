package net.choc.entry.dao;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;


class MyStateSerializerTest {
    @Test
    public void test1() throws Exception {
        int bufferLen = 0x4;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(bufferLen);

        MyState state = new MyState(1,2,3,4);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(state);
        objectOutputStream.flush();

        byte[] buffer = outputStream.toByteArray();

        ByteArrayInputStream inputStream = new ByteArrayInputStream(buffer);
        ObjectInputStream inputStream1 = new ObjectInputStream(inputStream);

        MyState state2 = (MyState) inputStream1.readObject();

        Assertions.assertEquals(state, state2);
    }
}