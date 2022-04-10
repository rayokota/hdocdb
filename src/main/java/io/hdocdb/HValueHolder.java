package io.hdocdb;

import org.ojai.FieldPath;
import org.ojai.Value;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Objects;

public class HValueHolder implements Externalizable {

    protected HValue value;

    public HValueHolder() {
    }

    public HValueHolder(HValue value) {
        this.value = value;
    }

    public HValue getValue() {
        return value;
    }

    public void setValue(HValue value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HValueHolder that = (HValueHolder) o;

        return Objects.equals(value, that.value);

    }

    @Override
    public int hashCode() {
        return value != null ? value.hashCode() : 0;
    }

    public void readExternal(ObjectInput input) throws IOException {
        this.value = readValueExternal(input);
    }

    private HValue readValueExternal(ObjectInput input) throws IOException {
        byte code = input.readByte();
        Value.Type type = Value.Type.valueOf(code);
        if (type == Value.Type.ARRAY) {
            return readArrayExternal(input);
        } else if (type == Value.Type.MAP) {
            return readMapExternal(input);
        } else {
            int len = input.readInt();
            byte[] value = new byte[len];
            if (len > 0) input.readFully(value, 0, len);
            return new HValue(type, value);
        }
    }

    private HList readArrayExternal(ObjectInput input) throws IOException {
        HList list = new HList();
        int size = input.readInt();
        for (int i = 0; i < size; i++) {
            list.set(i, readValueExternal(input));
        }
        return list;
    }

    private HDocument readMapExternal(ObjectInput input) throws IOException {
        HDocument doc = new HDocument();
        int size = input.readInt();
        for (int i = 0; i < size; i++) {
            String key = input.readUTF();
            HValue value = readValueExternal(input);
            doc.set(FieldPath.parseFrom(key), value);
        }
        return doc;
    }

    public void writeExternal(ObjectOutput output) throws IOException {
        writeValueExternal(output, value);
    }

    private void writeValueExternal(ObjectOutput output, HValue value) throws IOException {
        Value.Type type = value.getType();
        output.writeByte(type.getCode());
        if (type == Value.Type.MAP) {
            writeMapExternal(output, (HDocument) value);
        } else if (type == Value.Type.ARRAY) {
            writeArrayExternal(output, (HList) value);
        } else {
            byte[] rawBytes = value.getRawBytes();
            int len = rawBytes.length;
            output.writeInt(len);
            if (len > 0) output.write(rawBytes);
        }
    }

    private void writeArrayExternal(ObjectOutput output, HList list) throws IOException {
        output.writeInt(list.size());
        for (HValue value : list.getHValues()) {
            writeValueExternal(output, value);
        }
    }

    private void writeMapExternal(ObjectOutput output, HDocument doc) throws IOException {
        output.writeInt(doc.size());
        for (Map.Entry<String, Value> entry : doc) {
            output.writeUTF(entry.getKey());
            writeValueExternal(output, (HValue) entry.getValue());
        }
    }
}
