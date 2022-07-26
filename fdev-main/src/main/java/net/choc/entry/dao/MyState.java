package net.choc.entry.dao;

import lombok.Data;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@Data
public class MyState implements java.io.Serializable {
    private static final long serialVersionUID = 77;

    private Integer i1;
    private Integer i2;
    private Integer i3;
    private Integer i4;

    public MyState() {}

    public MyState(int a, int b, int c, int d) {
        i1 = a;
        i2 = b;
        i3 = c;
        i4 = d;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        byte[] buffer = new byte[4];
        buffer[0] = i1.byteValue();
        buffer[1] = i2.byteValue();
        buffer[2] = i3.byteValue();
        buffer[3] = i4.byteValue();
        out.write(buffer);
    }

    private void readObject(ObjectInputStream in) throws IOException {
        byte[] buffer = new byte[4];
        in.read(buffer);
        i1 = (int) buffer[0];
        i2 = (int) buffer[1];
        i3 = (int) buffer[2];
        i4 = (int) buffer[3];
    }
}
