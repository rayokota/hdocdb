package io.hdocdb.util;

import java.io.*;

public class Codec<T extends Externalizable> {

    public byte[] encode(T value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        value.writeExternal(oos);
        oos.close();
        return baos.toByteArray();
    }

    public T decode(byte[] bytes, T value) throws IOException, ClassNotFoundException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        value.readExternal(ois);
        ois.close();
        return value;
    }
}
