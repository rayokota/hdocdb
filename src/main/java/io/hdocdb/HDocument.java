package io.hdocdb;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.*;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.DocumentReader;
import org.ojai.FieldPath;
import org.ojai.FieldSegment;
import org.ojai.Value;
import org.ojai.beans.BeanCodec;
import org.ojai.exceptions.DecodingException;
import org.ojai.exceptions.TypeException;
import org.ojai.json.Json;
import org.ojai.json.JsonOptions;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

public class HDocument extends HContainer implements Document, Map<String, Object> {

    public static final String ID = "_id";
    public static final String TS = "_ts";
    public static final FieldPath ID_PATH = FieldPath.parseFrom(ID);
    public static final FieldPath TS_PATH = FieldPath.parseFrom(TS);

    private HValue _id;
    private long ts = 0L;
    private boolean compareValueTimestamps = false;

    private Map<String, Value> entries = Maps.newLinkedHashMap();

    public HDocument() {
        setType(Type.MAP);
    }

    public HDocument(Result result) {
        compareValueTimestamps = true;
        try {
            setType(Type.MAP);
            Codec<HValueHolder> codec = new Codec<>();
            byte[] rowKey = result.getRow();
            if (rowKey != null) {
                setId(codec.decode(rowKey, new HValueHolder()).getValue());
                //System.out.println("Get id " + getId());
                for (Cell cell : result.listCells()) {
                    String path = Bytes.toString(CellUtil.cloneQualifier(cell));
                    HValue value = codec.decode(CellUtil.cloneValue(cell), new HValueHolder()).getValue();
                    value.setTs(cell.getTimestamp());
                    //System.out.println("Get ts " + cell.getTimestamp());
                    if (path.equals(TS)) {
                        setTs(value.getTimestampAsLong());
                    } else {
                        //System.out.println("Get " + path + ", value " + value);
                        setHValue(FieldPath.parseFrom(path).iterator(), value);
                    }
                }
            }
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    public HDocument(List<Cell> cells) {
        compareValueTimestamps = true;
        try {
            setType(Type.MAP);
            Codec<HValueHolder> codec = new Codec<>();
            boolean foundId = false;
            for (Cell cell : cells) {
                if (!foundId) {
                    setId(codec.decode(CellUtil.cloneRow(cell), new HValueHolder()).getValue());
                    foundId = true;
                    //System.out.println("Get id " + getId());
                }
                String path = Bytes.toString(CellUtil.cloneQualifier(cell));
                HValue value = codec.decode(CellUtil.cloneValue(cell), new HValueHolder()).getValue();
                value.setTs(cell.getTimestamp());
                //System.out.println("Get ts " + cell.getTimestamp());
                if (path.equals(TS)) {
                    setTs(value.getTimestampAsLong());
                } else {
                    //System.out.println("Get " + path + ", value " + value);
                    setHValue(FieldPath.parseFrom(path).iterator(), value);
                }
            }
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    /**
     * Sets the the "_id" field of this Document to the specified Value.
     *
     * @param _id Value to set as the value of "_id" field
     * @return {@code this} for chaining
     */
    public Document setId(Value _id) {
        HValue id = HValue.initFromValue(_id);
        this._id = id;
        return this;
    }

    /**
     * @return The "_id" field of this Document
     */
    public Value getId() {
        return _id;
    }

    /**
     * Sets the the "_id" field of this Document to the specified string.
     *
     * @param _id String to set as the value of the "_id" field
     * @return {@code this} for chaining
     */
    public Document setId(String _id) {
        HValue id = new HValue(_id);
        this._id = id;
        return this;
    }

    /**
     * @return the String _id of this document
     * @throws TypeException if the _id of this Document is not of the String type
     */
    public String getIdString() {
        return _id.getString();
    }

    /**
     * Sets the the "_id" field of this Document to the specified string.
     *
     * @param _id ByteBuffer to set as the value of "_id" field
     * @return {@code this} for chaining
     */
    public Document setId(ByteBuffer _id) {
        HValue id = new HValue(_id);
        this._id = id;
        return this;
    }

    /**
     * @return the binary _id of this document
     * @throws TypeException if the _id of this Document is not of the BINARY type
     */
    public ByteBuffer getIdBinary() {
        return _id.getBinary();
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    protected boolean compareValueTimestamps() {
        return compareValueTimestamps;
    }

    protected void setCompareValueTimestamps(boolean compare) {
        this.compareValueTimestamps = compare;
    }

    /**
     * Returns {@code true} if this Document does not support any write
     * operations like set/delete etc.
     */
    public boolean isReadOnly() {
        return false;
    }

    /**
     * @return The number of top level entries in the document.
     */
    public int size() {
        return entries.size() + (_id != null ? 1 : 0);
    }

    /**
     * Converts this Document to an instance of the specified class.
     *
     * @param beanClass the class of instance
     * @return An instance of the specified class converted from this Document
     */
    public <T> T toJavaBean(Class<T> beanClass) throws DecodingException {
        return BeanCodec.encode(this.asReader(), beanClass);
    }

    /**
     * Removes all of the entries from this document.
     */
    public Document empty() {
        clearHValues();
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified String.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the String value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, String value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified String.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the String value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, String value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified boolean value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the boolean value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, boolean value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified boolean value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the boolean value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, boolean value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified byte value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the byte value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, byte value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified byte value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the byte value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, byte value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified short value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the short value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, short value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified short value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the short value
     * @return {@code this} for chaining.
     */
    public Document set(FieldPath fieldPath, short value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified int value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the int value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, int value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified int value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the int value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, int value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified long value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the long value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, long value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified long value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the long value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, long value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified float value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the float value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, float value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified float value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the float value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, float value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified double value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the double value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, double value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified double value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the double value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, double value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified BigDecimal.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the BigDecimal value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, BigDecimal value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified BigDecimal.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the BigDecimal value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, BigDecimal value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Time.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Time value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, OTime value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Time.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Time value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, OTime value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Date.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Date value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, ODate value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Date.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Date value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, ODate value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Timestamp.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Timestamp value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, OTimestamp value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Timestamp.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Timestamp value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, OTimestamp value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Interval.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Interval value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, OInterval value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Interval.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Interval value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, OInterval value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified binary value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the byte array containing the binary value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, byte[] value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified binary value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the byte array containing the binary value
     * @return {@code this} for chaining.
     */
    public Document set(FieldPath fieldPath, byte[] value) {
        setHValue(fieldPath, new HValue(ByteBuffer.wrap(value)));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified binary value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the byte array containing the binary value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, byte[] value, int off, int len) {
        return set(FieldPath.parseFrom(fieldPath), value, off, len);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified binary value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the byte array containing the binary value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, byte[] value, int off, int len) {
        setHValue(fieldPath, new HValue(ByteBuffer.wrap(value, off, len)));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified ByteBuffer.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the ByteBuffer
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, ByteBuffer value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified ByteBuffer.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the ByteBuffer
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, ByteBuffer value) {
        setHValue(fieldPath, new HValue(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Map.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Map value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, Map<String, ?> value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Map.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Map value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, Map<String, ?> value) {
        setHValue(fieldPath, initFromMap(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Document.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Document
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, Document value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Document.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Document
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, Document value) {
        setHValue(fieldPath, initFromDocument(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Value
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, Value value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Value.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Value
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, Value value) {
        setHValue(fieldPath, initFromValue(value));
        return this;
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Object List.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Object List
     * @return {@code this} for chaining
     */
    public Document set(String fieldPath, List<?> value) {
        return set(FieldPath.parseFrom(fieldPath), value);
    }


    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Object List.
     *
     * @param fieldPath the FieldPath to set
     * @param value     the Object List
     * @return {@code this} for chaining
     */
    public Document set(FieldPath fieldPath, List<?> value) {
        setHValue(fieldPath, initFromList(value));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified boolean array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the boolean array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, boolean[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified boolean array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the boolean array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, boolean[] values) {
        setHValue(fieldPath, initFromList(Booleans.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified byte array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the byte array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, byte[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified byte array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the byte array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, byte[] values) {
        setHValue(fieldPath, initFromList(com.google.common.primitives.Bytes.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified short array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the short array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, short[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified short array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the short array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, short[] values) {
        setHValue(fieldPath, initFromList(Shorts.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified int array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the int array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, int[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified int array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the int array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, int[] values) {
        setHValue(fieldPath, initFromList(Ints.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified long array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the long array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, long[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified long array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the long array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, long[] values) {
        setHValue(fieldPath, initFromList(Longs.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified float array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the float array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, float[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified float array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the float array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, float[] values) {
        setHValue(fieldPath, initFromList(Floats.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified double array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the double array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, double[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified double array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the double array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, double[] values) {
        setHValue(fieldPath, initFromList(Doubles.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified String array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the String array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, String[] values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified String array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the String array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, String[] values) {
        setHValue(fieldPath, initFromList(Arrays.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Object array.
     *
     * @param fieldPath the FieldPath to set
     * @param values    the Object array
     * @return {@code this} for chaining
     */
    public Document setArray(String fieldPath, Object... values) {
        return setArray(FieldPath.parseFrom(fieldPath), values);
    }

    /**
     * Sets the value of the specified fieldPath in this Document to the
     * specified Object array.
     *
     * @param fieldPath the FieldPath to set
     * @param values the Object array
     * @return {@code this} for chaining
     */
    public Document setArray(FieldPath fieldPath, Object... values) {
        setHValue(fieldPath, initFromList(Arrays.asList(values)));
        return this;
    }

    /**
     * Sets the value of the specified fieldPath in this Document to
     * {@link Type#NULL}.
     *
     * @param fieldPath the FieldPath to set
     * @return {@code this} for chaining
     */
    public Document setNull(String fieldPath) {
        return setNull(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Sets the value of the specified fieldPath in this Document to
     * {@link Type#NULL}.
     *
     * @param fieldPath the FieldPath to set
     * @return {@code this} for chaining
     */
    public Document setNull(FieldPath fieldPath) {
        setHValue(fieldPath, HValue.NULL);
        return this;
    }


    /**
     * Deletes the value at the specified {@code FieldPath} if it exists.
     *
     * @param fieldPath The {@code fieldPath} to delete from the document
     * @return {@code this} for chaining
     */
    public Document delete(String fieldPath) {
        return delete(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Deletes the value at the specified {@code FieldPath} if it exists.
     *
     * @param fieldPath the {@code fieldPath} to delete from the document
     * @return {@code this} for chaining
     */
    public Document delete(FieldPath fieldPath) {
        removeHValue(fieldPath);
        return this;
    }


    /**
     * Returns the value at the specified fieldPath as a {@code String}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>STRING</code> type
     */
    public String getString(String fieldPath) {
        return getString(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code String}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>STRING</code> type
     */
    public String getString(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getString();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code boolean}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                <code>BOOLEAN</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public boolean getBoolean(String fieldPath) {
        return getBoolean(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code boolean}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                <code>BOOLEAN</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public boolean getBoolean(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getBoolean();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Boolean}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>BOOLEAN</code> type
     */
    public Boolean getBooleanObj(String fieldPath) {
        return getBooleanObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Boolean}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>BOOLEAN</code> type
     */
    public Boolean getBooleanObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getBoolean();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code byte}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>BYTE</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public byte getByte(String fieldPath) {
        return getByte(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code byte}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>BYTE</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public byte getByte(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getByte();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Byte}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>BYTE</code> type
     */
    public Byte getByteObj(String fieldPath) {
        return getByteObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Byte}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>BYTE</code> type
     */
    public Byte getByteObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getByte();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code short}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>SHORT</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public short getShort(String fieldPath) {
        return getShort(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code short}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>SHORT</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public short getShort(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getShort();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Short}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>SHORT</code> type
     */
    public Short getShortObj(String fieldPath) {
        return getShortObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Short}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>SHORT</code> type
     */
    public Short getShortObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getShort();
    }


    /**
     * Returns the value at the specified fieldPath as an {@code int}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>INT</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public int getInt(String fieldPath) {
        return getInt(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as an {@code int}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>INT</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public int getInt(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getInt();
    }


    /**
     * Returns the value at the specified fieldPath as an {@code Integer}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>INT</code> type
     */
    public Integer getIntObj(String fieldPath) {
        return getIntObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as an {@code Integer}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>INT</code> type
     */
    public Integer getIntObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getInt();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code long}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>LONG</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public long getLong(String fieldPath) {
        return getLong(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code long}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>LONG</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public long getLong(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getLong();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Long}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>LONG</code> type
     */
    public Long getLongObj(String fieldPath) {
        return getLongObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Long}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>LONG</code> type
     */
    public Long getLongObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getLong();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code float}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>FLOAT</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public float getFloat(String fieldPath) {
        return getFloat(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code float}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>FLOAT</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public float getFloat(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getFloat();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Float}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>FLOAT</code> type
     */
    public Float getFloatObj(String fieldPath) {
        return getFloatObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Float}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>FLOAT</code> type
     */
    public Float getFloatObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getFloat();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code double}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>DOUBLE</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public double getDouble(String fieldPath) {
        return getDouble(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code double}.
     *
     * @throws TypeException          if the value at the fieldPath is not of
     *                                the <code>DOUBLE</code> type
     * @throws NoSuchElementException if the specified field does not
     *                                exist in the {@code Document}
     */
    public double getDouble(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) throw new NoSuchElementException("Invalid field: " + fieldPath);
        return value.getDouble();
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Double}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>DOUBLE</code> type
     */
    public Double getDoubleObj(String fieldPath) {
        return getDoubleObj(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@code Double}
     * object or {@code null} if the specified {@code FieldPath} does
     * not exist in the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>DOUBLE</code> type
     */
    public Double getDoubleObj(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getDouble();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link BigDecimal}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>DECIMAL</code> type
     */
    public BigDecimal getDecimal(String fieldPath) {
        return getDecimal(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link BigDecimal}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>DECIMAL</code> type
     */
    public BigDecimal getDecimal(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getDecimal();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link OTime}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>TIME</code> type
     */
    public OTime getTime(String fieldPath) {
        return getTime(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link OTime}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>TIME</code> type
     */
    public OTime getTime(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getTime();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link ODate}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>DATE</code> type
     */
    public ODate getDate(String fieldPath) {
        return getDate(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link ODate}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>DATE</code> type
     */
    public ODate getDate(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getDate();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link OTimestamp}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>TIMESTAMP</code> type
     */
    public OTimestamp getTimestamp(String fieldPath) {
        return getTimestamp(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link OTimestamp}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>TIMESTAMP</code> type
     */
    public OTimestamp getTimestamp(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getTimestamp();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link ByteBuffer}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>BINARY</code> type
     */
    public ByteBuffer getBinary(String fieldPath) {
        return getBinary(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link ByteBuffer}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       <code>BINARY</code> type
     */
    public ByteBuffer getBinary(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getBinary();
    }


    /**
     * Returns the value at the specified fieldPath as an {@link OInterval}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>INTERVAL</code> type
     */
    public OInterval getInterval(String fieldPath) {
        return getInterval(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as an {@link OInterval}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>INTERVAL</code> type
     */
    public OInterval getInterval(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getInterval();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link Value}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     */
    public Value getValue(String fieldPath) {
        return getValue(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link Value}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     */
    public Value getValue(FieldPath fieldPath) {
        return getHValue(fieldPath);
    }


    /**
     * Returns the value at the specified fieldPath as a {@link Map}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>MAP</code> type
     */
    public Map<String, Object> getMap(String fieldPath) {
        return getMap(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link Map}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>MAP</code> type
     */
    public Map<String, Object> getMap(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getMap();
    }


    /**
     * Returns the value at the specified fieldPath as a {@link List}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>ARRAY</code> type
     */
    public List<Object> getList(String fieldPath) {
        return getList(FieldPath.parseFrom(fieldPath));
    }


    /**
     * Returns the value at the specified fieldPath as a {@link List}
     * object or {@code null} if the specified {@code FieldPath} does not
     * exist in the document. Modifying the returned object does not alter the
     * content of the document.
     *
     * @throws TypeException if the value at the fieldPath is not of
     *                       the <code>ARRAY</code> type
     */
    public List<Object> getList(FieldPath fieldPath) {
        Value value = getHValue(fieldPath);
        if (value == null) return null;
        return value.getList();
    }


    /**
     * @return This Document serialized as Json string using the default options.
     */
    @Override
    public String toString() {
        return asJsonString();
    }


    /**
     * @return This Document serialized as Json string using the default options.
     */
    public String asJsonString() {
        return asJsonString(JsonOptions.DEFAULT);
    }


    /**
     * @return This Document serialized as Json string using the specified options
     */
    public String asJsonString(JsonOptions options) {
        return Json.toJsonString(this, options);
    }


    /**
     * @return A new {@link DocumentReader} over the current <code>document</code>
     */
    public DocumentReader asReader() {
        return new HDocumentReader(this);
    }


    /**
     * @return A new {@link DocumentReader} over the node specified by the
     * fieldPath or <code>null</code> if the node does not exist
     */
    public DocumentReader asReader(String fieldPath) {
        return asReader(FieldPath.parseFrom(fieldPath));
    }


    /**
     * @return A new {@link DocumentReader} over the node specified by the
     * fieldPath or <code>null</code> if the node does not exist
     */
    public DocumentReader asReader(FieldPath fieldPath) {
        return new HDocumentReader(getHValue(fieldPath));
    }


    /**
     * @return A new {@link Map} representing the Document.
     */
    public Map<String, Object> asMap() {
        Map<String, Object> result = Maps.newLinkedHashMap();
        if (_id != null) result.put(ID, _id.getObject());
        for (Map.Entry<String, Value> entry : entries.entrySet()) {
            result.put(entry.getKey(), HValue.initFromValue(entry.getValue()).getObject());
        }
        return result;
    }

    public Iterator<Entry<String, Value>> iterator() {
        Map<String, Value> result = Maps.newLinkedHashMap();
        if (_id != null) result.put(ID, _id);
        result.putAll(entries);
        return result.entrySet().iterator();
    }

    public boolean containsKey(Object key) {
        if (_id != null && ID.equals(key)) return true;
        return entries.containsKey(key);
    }

    public boolean containsValue(Object value) {
        HValue v = HValue.initFromObject(value);
        if (_id != null && _id.equals(v)) return true;
        return entries.containsValue(v);
    }

    public Set<Entry<String, Object>> entrySet() {
        return asMap().entrySet();
    }

    public Object get(Object key) {
        if (_id != null && ID.equals(key)) return _id.getObject();
        HValue value = (HValue) entries.get(key);
        return value != null ? value.getObject() : null;
    }

    public boolean isEmpty() {
        return _id == null && entries.isEmpty();
    }

    public Set<String> keySet() {
        Set<String> result = Sets.newLinkedHashSet();
        if (_id != null) result.add(ID);
        result.addAll(entries.keySet());
        return result;

    }

    public Object put(String key, Object value) {
        Object oldValue = get(key);
        set(key, HValue.initFromObject(value));
        return oldValue;
    }

    public void putAll(Map<? extends String, ?> m) {
        for (Map.Entry<? extends String, ?> entry : m.entrySet()) {
            put(entry.getKey(), entry.getValue());
        }
    }

    public Object remove(Object key) {
        Object oldValue = get(key);
        delete(key.toString());
        return oldValue;
    }

    public Collection<Object> values() {
        ArrayList<Object> result = new ArrayList<>();
        if (_id != null) result.add(_id.getObject());
        for (Value value : entries.values()) {
            result.add(value.getObject());
        }
        return result;
    }

    public HDocument shallowCopy() {
        HDocument document = new HDocument();
        document._id = this._id;
        document.entries = entries;
        document.ts = this.ts;
        document.compareValueTimestamps = this.compareValueTimestamps;
        document.type = this.type;
        document.value = this.value;
        return document;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HDocument entries1 = (HDocument) o;

        if (!Objects.equals(_id, entries1._id)) return false;
        return entries.equals(entries1.entries);
    }

    @Override
    public int hashCode() {
        int result = (_id != null ? _id.hashCode() : 0);
        result = 31 * result + entries.hashCode();
        return result;
    }

    public HValue checkHValue(Iterator<FieldSegment> path, HValue value) throws StoreException {
        FieldSegment field = path.next();
        if (field == null) return null;
        String key = field.getNameSegment().getName();
        if (Strings.isNullOrEmpty(key)) return this;
        if (key.equals(ID)) {
            if (!field.isLastPath()) throw new IllegalArgumentException("_id must be last on path");
            if (!_id.isSettableWithType(value.getType())) {
                throw new StoreException("New type " + value.getType() + " does not match existing type " + getId().getType());
            }
            return _id;
        }
        HValue oldValue = (HValue) entries.get(key);
        if (oldValue == null) return null;
        if (field.isLastPath()) {
            if (!oldValue.isSettableWithType(value.getType())) {
                throw new StoreException("New type " + value.getType() + " does not match existing type " + oldValue.getType());
            }
            return oldValue;
        } else if (field.isMap()) {
            if (!oldValue.isSettableWithType(Type.MAP)) {
                throw new StoreException("Map " + field + " does not match existing type " + oldValue.getType());
            }
            HDocument doc = (HDocument) oldValue;
            return doc.checkHValue(path, value);
        } else if (field.isArray()) {
            if (!oldValue.isSettableWithType(Type.ARRAY)) {
                throw new StoreException("Array " + field + " does not match existing type " + oldValue.getType());
            }
            HList list = (HList) oldValue;
            return list.checkHValue(path, value);
        }
        return null;
    }

    public HValue getHValue(Iterator<FieldSegment> path) {
        FieldSegment field = path.next();
        if (field == null) return null;
        String key = field.getNameSegment().getName();
        if (Strings.isNullOrEmpty(key)) return this;  // return this for ""
        if (key.equals(ID)) {
            if (!field.isLastPath()) throw new IllegalArgumentException("_id must be last on path");
            return _id;
        }
        HValue value = (HValue) entries.get(key);
        if (value == null || field.isLastPath()) {
            return value;
        } else if (field.isMap()) {
            if (value.getType() != Type.MAP) return null;
            HDocument doc = (HDocument) value;
            return doc.getHValue(path);
        } else if (field.isArray()) {
            if (value.getType() != Type.ARRAY) return null;
            HList list = (HList) value;
            return list.getHValue(path);

        }
        return null;
    }

    public void setHValue(Iterator<FieldSegment> path, HValue value) {
        FieldSegment field = path.next();
        if (field == null) return;
        String key = field.getNameSegment().getName();
        if (Strings.isNullOrEmpty(key)) return;
        if (key.equals(ID)) {
            if (!field.isLastPath()) throw new IllegalArgumentException("_id must be last on path");
            setId(value);
            return;
        }
        HValue oldValue = (HValue) entries.get(key);
        if (field.isLastPath()) {
            if (compareValueTimestamps && value.hasNewerTs(oldValue)) return;
            entries.put(key, value);
        } else if (field.isMap()) {
            if (oldValue != null && oldValue.getType() == Type.MAP) {
                HDocument doc = (HDocument) oldValue;
                doc.setCompareValueTimestamps(compareValueTimestamps);
                doc.setHValue(path, value);
                doc.setTs(value.getTs());
            } else {
                if (compareValueTimestamps && value.hasNewerTs(oldValue)) return;
                HDocument doc = new HDocument();
                doc.setCompareValueTimestamps(compareValueTimestamps);
                doc.setHValue(path, value);
                doc.setTs(value.getTs());
                entries.put(key, doc);
            }
        } else if (field.isArray()) {
            if (oldValue != null && oldValue.getType() == Type.ARRAY) {
                HList list = (HList) oldValue;
                list.setCompareValueTimestamps(compareValueTimestamps);
                list.setHValue(path, value);
                list.setTs(value.getTs());
            } else {
                if (compareValueTimestamps && value.hasNewerTs(oldValue)) return;
                HList list = new HList();
                list.setCompareValueTimestamps(compareValueTimestamps);
                list.setHValue(path, value);
                list.setTs(value.getTs());
                entries.put(key, list);
            }
        }
    }

    public void removeHValue(Iterator<FieldSegment> path) {
        FieldSegment field = path.next();
        if (field == null) return;
        String key = field.getNameSegment().getName();
        if (Strings.isNullOrEmpty(key)) return;
        if (key.equals(ID)) {
            if (!field.isLastPath()) throw new IllegalArgumentException("_id must be last on path");
            _id = null;
            return;
        }
        HValue oldValue = (HValue) entries.get(key);
        if (oldValue == null) return;
        if (field.isLastPath()) {
            entries.remove(key);
        } else if (field.isMap()) {
            if (oldValue.getType() == Type.MAP) {
                HDocument doc = (HDocument) oldValue;
                doc.removeHValue(path);
            }
        } else if (field.isArray()) {
            if (oldValue.getType() == Type.ARRAY) {
                HList list = (HList) oldValue;
                list.removeHValue(path);
            }
        }
    }

    public void clearHValues() {
        _id = null;
        // don't call entries.clear() due to possible sharing from shallowCopy()
        entries = Maps.newLinkedHashMap();
    }

    protected HDocument reindexArrays() {
        for (String key : Sets.newLinkedHashSet(entries.keySet())) {
            Value value = entries.get(key);
            if (value.getType() == Type.MAP) {
                HDocument doc = ((HDocument)value).reindexArrays();
                entries.put(key, doc);
            }
            else if (value.getType() == Type.ARRAY) {
                HList list = ((HList)value).reindexArrays();
                entries.put(key, list);
            }
        }
        return this;
    }

    public void fillDelete(Delete delete, String family, FieldPath path) {
        for (Map.Entry<String, Value> entry : entries.entrySet()) {
            HValue value = (HValue) entry.getValue();
            value.fillDelete(delete, family, value.getFullPath(path, entry.getKey()));
        }
    }

    public void fillPut(Put put, String family, FieldPath path) {
        for (Map.Entry<String, Value> entry : entries.entrySet()) {
            HValue value = (HValue) entry.getValue();
            value.fillPut(put, family, value.getFullPath(path, entry.getKey()));
        }
    }
}
