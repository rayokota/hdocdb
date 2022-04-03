package io.hdocdb;

import com.google.common.base.Strings;
import com.google.common.primitives.*;
import io.hdocdb.store.Order;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.ojai.Document;
import org.ojai.DocumentReader;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.exceptions.TypeException;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;
import org.ojai.util.Fields;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HValue implements Value, Comparable<HValue> {

    public static final HValue NULL = new HValue();

    protected Value.Type type;
    protected byte[] value;
    protected long ts = 0L;

    protected HValue(Value.Type type, byte[] value) {
        this.type = type;
        this.value = value != null ? Arrays.copyOf(value, value.length) : new byte[0];
    }

    public HValue() {
        this(Type.NULL, null);
    }

    public HValue(boolean value) {
        this(Type.BOOLEAN, Bytes.toBytes(value));
    }

    public HValue(String value) {
        this(Type.STRING, Bytes.toBytes(value));
    }

    public HValue(byte value) {
        this(Type.BYTE, new byte[] { value });
    }

    public HValue(short value) {
        this(Type.SHORT, Bytes.toBytes(value));
    }

    public HValue(int value) {
        this(Type.INT, Bytes.toBytes(value));
    }

    public HValue(long value) {
        this(Type.LONG, Bytes.toBytes(value));
    }

    public HValue(float value) {
        this(Type.FLOAT, Bytes.toBytes(value));
    }

    public HValue(double value) {
        this(Type.DOUBLE, Bytes.toBytes(value));
    }

    public HValue(OTime value) {
        this(Type.TIME, Bytes.toBytes(value.toTimeInMillis()));
    }

    public HValue(ODate value) {
        this(Type.DATE, Bytes.toBytes(value.toDaysSinceEpoch()));
    }

    public HValue(BigDecimal value) {
        this(Type.DECIMAL, Bytes.toBytes(value));
    }

    public HValue(OTimestamp value) {
        this(Type.TIMESTAMP, Bytes.toBytes(value.getMillis()));
    }

    public HValue(OInterval value) {
        this(Type.INTERVAL, Bytes.toBytes(value.getTimeInMillis()));
    }

    public HValue(ByteBuffer value) {
        this(Type.BINARY, Bytes.toBytes(value));
    }

    public static HList initFromList(List list) {
        if (list instanceof HList) {
            return ((HList)list).shallowCopy();
        } else {
            HList result = new HList();
            Iterator itr = list.iterator();

            int index = 0;
            while (itr.hasNext()) {
                Object o = itr.next();
                HValue child = initFromObject(o);
                result.set(index, child);
                index++;
            }

            return result;
        }
    }

    public static HDocument initFromMap(Map<String, ? extends Object> map) {
        if (map instanceof HDocument) {
            return ((HDocument)map).shallowCopy();
        } else {
            HDocument result = new HDocument();

            for (Map.Entry<String, ? extends Object> entry : map.entrySet()) {
                HValue child = initFromObject(entry.getValue());
                result.set(Fields.quoteFieldName(entry.getKey()), child);
            }

            return result;
        }
    }

    public static HDocument initFromDocument(Document value) {
        if (value instanceof HDocument) {
            return ((HDocument)value).shallowCopy();
        } else {
            HDocument result = new HDocument();

            for (Map.Entry<String, Value> e : value) {
                HValue child = initFromObject(e.getValue());
                result.set(Fields.quoteFieldName(e.getKey()), child);
            }

            return result;
        }
    }

    public static HValue initFromJson(org.graalvm.polyglot.Value json) {
        // TODO date
        if (json.isNull()) {
            return HValue.NULL;
        } else if (json.isBoolean()) {
            return new HValue(json.asBoolean());
        } else if (json.isNumber()) {
            return new HValue(json.asDouble());
        } else if (json.isString()) {
            return new HValue(json.asString());
        } else if (json.hasArrayElements()) {
            HList result = new HList();
            for (int i = 0; i < json.getArraySize(); i++) {
                result.set(i, initFromObject(json.getArrayElement(i)));
            }
            return result;
        } else if (json.hasMembers()) {
            HDocument result = new HDocument();
            for (String key : json.getMemberKeys()) {
                if (key.contains(".")) {
                    throw new IllegalArgumentException("Key names cannot contain dot (.)");
                }
                if (key.startsWith("$")) {
                    throw new IllegalArgumentException("Key names cannot start with $");
                }
                HValue child = initFromObject(json.getMember(key));
                result.set(Fields.quoteFieldName(key), child);
            }
            return result;
        }
        return null;
    }

    public static HValue initFromValue(Value value) {
        if (value == null) {
            return HValue.NULL;
        } else if (value instanceof HValue) {
            return ((HValue)value).shallowCopy();
        } else {
            return initFromObject(value.getObject());
        }
    }

    @SuppressWarnings("unchecked")
    public static HValue initFromObject(Object value) {
        if (value instanceof HValue) {
            return ((HValue)value).shallowCopy();
        } else if (value == null) {
            return HValue.NULL;
        } else if (value instanceof org.graalvm.polyglot.Value) {
            return initFromJson((org.graalvm.polyglot.Value)value);
        } else if (value instanceof Byte) {
            return new HValue((Byte)value);
        } else if (value instanceof Boolean) {
            return new HValue((Boolean)value);
        } else if (value instanceof String) {
            return new HValue((String)value);
        } else if (value instanceof Short) {
            return new HValue((Short)value);
        } else if (value instanceof Integer) {
            return new HValue((Integer)value);
        } else if (value instanceof Long) {
            return new HValue((Long)value);
        } else if (value instanceof Float) {
            return new HValue((Float)value);
        } else if (value instanceof Double) {
            return new HValue((Double)value);
        } else if (value instanceof OTime) {
            return new HValue((OTime)value);
        } else if (value instanceof ODate) {
            return new HValue((ODate)value);
        } else if (value instanceof OTimestamp) {
            return new HValue((OTimestamp)value);
        } else if (value instanceof BigDecimal) {
            return new HValue((BigDecimal)value);
        } else if (value instanceof ByteBuffer) {
            return new HValue((ByteBuffer)value);
        } else if (value instanceof OInterval) {
            return new HValue((OInterval)value);
        } else if (value instanceof Document) {
            return initFromDocument((Document)value);
        } else if (value instanceof Map) {
            return initFromMap((Map)value);
        } else if (value instanceof List) {
            return initFromList((List)value);
        } else if (value instanceof Value) {
            return initFromValue((Value)value);
        } else if (value instanceof boolean[]) {
            return initFromList(Booleans.asList((boolean[])value));
        } else if (value instanceof byte[]) {
            return initFromList(com.google.common.primitives.Bytes.asList((byte[])value));
        } else if (value instanceof short[]) {
            return initFromList(Shorts.asList((short[])value));
        } else if (value instanceof int[]) {
            return initFromList(Ints.asList((int[])value));
        } else if (value instanceof long[]) {
            return initFromList(Longs.asList((long[])value));
        } else if (value instanceof float[]) {
            return initFromList(Floats.asList((float[])value));
        } else if (value instanceof double[]) {
            return initFromList(Doubles.asList((double[])value));
        } else if (value.getClass().isArray()) {
            return initFromList(Arrays.asList(((Object[])value)));
        } else {
            throw new TypeException("Unsupported type: " + value.getClass());
        }
    }

    /**
     * @return The <code>Type</code> of this Value.
     */
    public Value.Type getType() {
        return type;
    }

    protected void setType(Value.Type type) {
        this.type = type;
    }

    protected boolean isSettableWithType(Value.Type newType) {
        switch (type) {
            case NULL:
                return true;
            case BOOLEAN:
            case STRING:
                return newType == type;
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return newType == Type.BYTE || newType == Type.SHORT || newType == Type.INT
                        || newType == Type.LONG || newType == Type.FLOAT || newType == Type.DOUBLE
                        || newType == Type.DECIMAL;
            case DATE:
            case TIME:
            case TIMESTAMP:
            case INTERVAL:
            case BINARY:
            case MAP:
            case ARRAY:
                return newType == type;
            default:
                throw new TypeException("Invalid type " + this.type);
        }
    }

    public byte[] getRawBytes() {
        return value;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    protected boolean hasNewerTs(HValue oldValue) {
        return oldValue != null && getTs() < oldValue.getTs();
    }

    /**
     * Returns the value as a <code>byte</code>.
     *
     * @throws TypeException if this value is not of <code>BYTE</code> type.
     */
    public byte getByte() {
        checkType(Type.BYTE);
        byte[] bytes = getRawBytes();
        if (bytes.length != 1) throw new TypeException("Invalid BYTE value");
        return bytes[0];
    }

    /**
     * Returns the value as a <code>short</code>.
     *
     * @throws TypeException if this value is not of <code>SHORT</code> type.
     */
    public short getShort() {
        checkType(Type.SHORT);
        return Bytes.toShort(getRawBytes());
    }

    /**
     * Returns the value as an {@code int}.
     *
     * @throws TypeException if this value is not of <code>INT</code> type.
     */
    public int getInt() {
        checkType(Type.INT);
        return Bytes.toInt(getRawBytes());
    }

    /**
     * Returns the value as a <code>long</code>.
     *
     * @throws TypeException if this value is not of <code>LONG</code> type.
     */
    public long getLong() {
        checkType(Type.LONG);
        return Bytes.toLong(getRawBytes());
    }

    /**
     * Returns the value as a <code>float</code>.
     *
     * @throws TypeException if this value is not of <code>FLOAT</code> type.
     */
    public float getFloat() {
        checkType(Type.FLOAT);
        return Bytes.toFloat(getRawBytes());
    }

    /**
     * Returns the value as a <code>double</code>.
     *
     * @throws TypeException if this value is not of <code>DOUBLE</code> type.
     */
    public double getDouble() {
        checkType(Type.DOUBLE);
        return Bytes.toDouble(getRawBytes());
    }

    /**
     * Returns the value as a <code>BigDecimal</code>.
     *
     * @throws TypeException if this value is not of Type.DECIMAL type.
     */
    public BigDecimal getDecimal() {
        checkType(Type.DECIMAL);
        return Bytes.toBigDecimal(getRawBytes());
    }

    /**
     * Returns the value as a <code>boolean</code>.
     *
     * @throws TypeException if this value is not of <code>BOOLEAN</code> type.
     */
    public boolean getBoolean() {
        checkType(Type.BOOLEAN);
        return Bytes.toBoolean(getRawBytes());
    }

    /**
     * Returns the value as a <code>String</code>.
     *
     * @throws TypeException if this value is not of <code>STRING</code> type.
     */
    public String getString() {
        checkType(Type.STRING);
        return Bytes.toString(getRawBytes());
    }

    /**
     * Returns the value as a {@link OTimestamp} object.
     *
     * @throws TypeException if this value is not of <code>TIMESTAMP</code> type.
     */
    public OTimestamp getTimestamp() {
        return new OTimestamp(getTimestampAsLong());
    }

    /**
     * Returns a long value representing the number of milliseconds since epoch.
     *
     * @throws TypeException if this value is not of <code>TIMESTAMP</code> type.
     */
    public long getTimestampAsLong() {
        checkType(Type.TIMESTAMP);
        return Bytes.toLong(getRawBytes());
    }

    /**
     * Returns the value as a {@link ODate} object.
     *
     * @throws TypeException if this value is not of <code>DATE</code> type.
     */
    public ODate getDate() {
        return ODate.fromDaysSinceEpoch(getDateAsInt());
    }

    /**
     * Returns a {@code int} representing the number of DAYS since Unix epoch.
     *
     * @throws TypeException if this value is not of <code>DATE</code> type.
     */
    public int getDateAsInt() {
        checkType(Type.DATE);
        return Bytes.toInt(getRawBytes());
    }

    /**
     * Returns the value as a {@link OTime} object. Modifying the
     * returned object does not alter the content of the Value.
     *
     * @throws TypeException if this value is not of <code>TIME</code> type.
     */
    public OTime getTime() {
        return OTime.fromMillisOfDay(getTimeAsInt());
    }

    /**
     * Returns a {@code int} representing the number of milliseconds since midnight.
     *
     * @throws TypeException if this value is not of <code>TIME</code> type.
     */
    public int getTimeAsInt() {
        checkType(Type.TIME);
        return Bytes.toInt(getRawBytes());
    }

    /**
     * Returns the value as a {@link OInterval} object.
     * Modifying the returned object does not alter the content of the Value.
     *
     * @throws TypeException if this value is not of <code>INTERVAL</code> type.
     */
    public OInterval getInterval() {
        return new OInterval(getIntervalAsLong());
    }

    /**
     * Returns a <code>long</code> representing interval duration in milliseconds.
     *
     * @throws TypeException if this value is not of <code>INTERVAL</code> type.
     */
    public long getIntervalAsLong() {
        checkType(Type.INTERVAL);
        return Bytes.toLong(getRawBytes());
    }

    /**
     * Returns the value as a {@link ByteBuffer}. Modifying the
     * returned object does not alter the content of the Value.
     *
     * @throws TypeException if this value is not of <code>BINARY</code> type.
     */
    public ByteBuffer getBinary() {
        checkType(Type.BINARY);
        return ByteBuffer.wrap(value);
    }

    /**
     * Returns the value as a <code>Map&lt;String, Object&gt;</code>. The returned
     * Map could be mutable or immutable however, modifying the returned Map
     * does not alter the content of the Value.
     *
     * @throws TypeException if this value is not of <code>MAP</code> type.
     */
    public Map<String, Object> getMap() {
        checkType(Type.MAP);
        return (HDocument) this;
    }

    /**
     * Returns the value as a <code>List&lt;Object&gt;</code>. The returned List
     * could be mutable or immutable however, modifying the returned List does
     * not alter the content of the Value.
     *
     * @throws TypeException If this value is not of <code>ARRAY</code> type.
     */
    public List<Object> getList() {
        checkType(Type.ARRAY);
        return (HList) this;
    }

    /**
     * Returns the value as an <code>Object</code> of the underlying type.
     */
    public Object getObject() {
        switch (type) {
            case NULL:
                return null;
            case BOOLEAN:
                return getBoolean();
            case STRING:
                return getString();
            case BYTE:
                return getByte();
            case SHORT:
                return getShort();
            case INT:
                return getInt();
            case LONG:
                return getLong();
            case FLOAT:
                return getFloat();
            case DOUBLE:
                return getDouble();
            case DECIMAL:
                return getDecimal();
            case DATE:
                return getDate();
            case TIME:
                return getTime();
            case TIMESTAMP:
                return getTimestamp();
            case INTERVAL:
                return getInterval();
            case BINARY:
                return getBinary();
            case MAP:
                return getMap();
            case ARRAY:
                return getList();
            default:
                throw new TypeException("Invalid type " + this.type);
        }
    }

    /**
     * Returns a {@link DocumentReader} over the current document.
     */
    public DocumentReader asReader() {
        return new HDocumentReader(this);
    }

    public boolean equals(Object obj) {
        if (obj == null) {
            return getType() == Type.NULL;
        } if (obj instanceof HValue) {
            return getType() == Type.NULL
                    ? ((HValue) obj).getType() == Type.NULL
                    : getObject().equals(((HValue) obj).getObject());
        } else if (obj instanceof Boolean) {
            return obj.equals(this.getBoolean());
        } else if (obj instanceof String) {
            return obj.equals(this.getString());
        } else if (obj instanceof Byte) {
            return obj.equals(this.getByte());
        } else if (obj instanceof Short) {
            return obj.equals(this.getShort());
        } else if (obj instanceof Integer) {
            return obj.equals(this.getInt());
        } else if (obj instanceof Long) {
            return obj.equals(this.getLong());
        } else if (obj instanceof Float) {
            return obj.equals(this.getFloat());
        } else if (obj instanceof Double) {
            return obj.equals(this.getDouble());
        } else if (obj instanceof BigDecimal) {
            return obj.equals(this.getDecimal());
        } else if (obj instanceof ODate) {
            return obj.equals(this.getDate());
        } else if (obj instanceof OTime) {
            return obj.equals(this.getTime());
        } else if (obj instanceof OTimestamp) {
            return obj.equals(this.getTimestamp());
        } else if (obj instanceof OInterval) {
            return obj.equals(this.getInterval());
        } else if (obj instanceof ByteBuffer) {
            return obj.equals(this.getBinary());
        } else if (obj instanceof Map) {
            return obj.equals(this.getMap());
        } else if (obj instanceof List) {
            return obj.equals(this.getList());
        } else {
            return false;
        }
    }

    public int hashCode() {
        return getObject().hashCode();
    }

    @SuppressWarnings("unchecked")
    public int compareTo(HValue that) {
        if (getType() != that.getType()) {
            throw new IllegalArgumentException("Cannot compare " + getType() + " with " + that.getType());
        }
        if (getType() == Type.MAP || getType() == Type.ARRAY) {
            throw new IllegalStateException("Cannot compare composite types");
        }
        Object o = getObject();
        if (o == null || !(o instanceof Comparable)) {
            throw new IllegalStateException("Type " + getType() + " is not comparable");
        }
        return ((Comparable)o).compareTo(that.getObject());
    }

    public String toString() {
        //return Values.asJsonString(this);
        return getObject() != null ? getObject().toString() : "null";
    }

    public HValue shallowCopy() {
        if (type == Type.MAP) {
            HDocument doc = (HDocument)this;
            return doc.shallowCopy();
        } else if (this.type == Type.ARRAY) {
            HList list = (HList)this;
            return list.shallowCopy();
        } else {
            HValue value = new HValue();
            value.type = this.type;
            value.value = this.value;
            return value;
        }
    }

    private void checkType(Type type) throws TypeException {
        if (this.type != type) {
            throw new TypeException("Value of type " + this.type + " does not match given type " + type);
        }
    }

    public void orderedDecode(PositionedByteRange pbr, Value.Type type) throws IOException {
        this.type = type;
        switch (type) {
            case NULL:
                if (OrderedBytes.isNull(pbr)) {
                    pbr.get();
                } else {
                    throw new IllegalArgumentException("Cannot decode NULL");
                }
                value = null;
                break;
            case BOOLEAN:
                value = Bytes.toBytes(OrderedBytes.decodeInt8(pbr) == 1);
                break;
            case STRING:
                value = Bytes.toBytes(OrderedBytes.decodeString(pbr));
                break;
            case BYTE:
                value = new byte[] { OrderedBytes.decodeInt8(pbr) };
                break;
            case SHORT:
                value = Bytes.toBytes(OrderedBytes.decodeInt16(pbr));
                break;
            case INT:
                value = Bytes.toBytes(OrderedBytes.decodeInt32(pbr));
                break;
            case LONG:
                value = Bytes.toBytes(OrderedBytes.decodeInt64(pbr));
                break;
            case FLOAT:
                value = Bytes.toBytes(OrderedBytes.decodeFloat32(pbr));
                break;
            case DOUBLE:
                value = Bytes.toBytes(OrderedBytes.decodeFloat64(pbr));
                break;
            case DECIMAL:
                value = Bytes.toBytes(OrderedBytes.decodeNumericAsBigDecimal(pbr));
                break;
            case DATE:
                value = Bytes.toBytes(OrderedBytes.decodeInt32(pbr));
                break;
            case TIME:
                value = Bytes.toBytes(OrderedBytes.decodeInt32(pbr));
                break;
            case TIMESTAMP:
                value = Bytes.toBytes(OrderedBytes.decodeInt64(pbr));
                break;
            case INTERVAL:
                value = Bytes.toBytes(OrderedBytes.decodeInt64(pbr));
                break;
            case BINARY:
                value = OrderedBytes.decodeBlobVar(pbr);
                break;
            case MAP:
                throw new IllegalArgumentException("Cannot decode map");
            case ARRAY:
                throw new IllegalArgumentException("Cannot decode array");
            default:
                throw new TypeException("Invalid type " + this.type);
        }
    }

    public void orderedEncode(PositionedByteRange pbr, Order order) throws IOException {
        org.apache.hadoop.hbase.util.Order horder = order == Order.ASCENDING
                ? org.apache.hadoop.hbase.util.Order.ASCENDING
                : org.apache.hadoop.hbase.util.Order.DESCENDING;
        switch (type) {
            case NULL:
                OrderedBytes.encodeNull(pbr, horder);
                break;
            case BOOLEAN:
                OrderedBytes.encodeInt8(pbr, getBoolean() ? (byte)1 : (byte)0, horder);
                break;
            case STRING:
                OrderedBytes.encodeString(pbr, getString(), horder);
                break;
            case BYTE:
                OrderedBytes.encodeInt8(pbr, getByte(), horder);
                break;
            case SHORT:
                OrderedBytes.encodeInt16(pbr, getShort(), horder);
                break;
            case INT:
                OrderedBytes.encodeInt32(pbr, getInt(), horder);
                break;
            case LONG:
                OrderedBytes.encodeInt64(pbr, getLong(), horder);
                break;
            case FLOAT:
                OrderedBytes.encodeFloat32(pbr, getFloat(), horder);
                break;
            case DOUBLE:
                OrderedBytes.encodeFloat64(pbr, getDouble(), horder);
                break;
            case DECIMAL:
                OrderedBytes.encodeNumeric(pbr, getDecimal(), horder);
                break;
            case DATE:
                OrderedBytes.encodeInt32(pbr, getDateAsInt(), horder);
                break;
            case TIME:
                OrderedBytes.encodeInt32(pbr, getTimeAsInt(), horder);
                break;
            case TIMESTAMP:
                OrderedBytes.encodeInt64(pbr, getTimestampAsLong(), horder);
                break;
            case INTERVAL:
                OrderedBytes.encodeInt64(pbr, getIntervalAsLong(), horder);
                break;
            case BINARY:
                OrderedBytes.encodeBlobVar(pbr, getRawBytes(), horder);
                break;
            case MAP:
                throw new IllegalArgumentException("Cannot encode map");
            case ARRAY:
                throw new IllegalArgumentException("Cannot encode array");
            default:
                throw new TypeException("Invalid type " + this.type);
        }
    }

    public FieldPath getFullPath(FieldPath parentPath, String key) {
        String parent = parentPath.asPathString();
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(parent)) sb.append(parent).append(".");
        if (!Strings.isNullOrEmpty(key)) sb.append(key);
        // return an actual FieldPath to ensure it is parseable
        return FieldPath.parseFrom(sb.toString());
    }

    public FieldPath getFullPath(FieldPath parentPath, int arrayIndex) {
        String parent = parentPath.asPathString();
        StringBuilder sb = new StringBuilder();
        if (!Strings.isNullOrEmpty(parent)) sb.append(parent);
        sb.append("[").append(arrayIndex).append("]");
        // return an actual FieldPath to ensure it is parseable
        return FieldPath.parseFrom(sb.toString());
    }

    public void fillDelete(Delete delete, String family, FieldPath path) {
        try {
            delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(path.asPathString()));
            //System.out.println("Delete " + path + ", value " + this);
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    public void fillPut(Put put, String family, FieldPath path) {
        try {
            Codec<HValueHolder> codec = new Codec<>();
            byte[] bytes = codec.encode(new HValueHolder(this));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(path.asPathString()), bytes);
            //System.out.println("Put " + path + ", value " + this);
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }
}
