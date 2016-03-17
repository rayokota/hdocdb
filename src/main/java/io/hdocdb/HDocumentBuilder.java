package io.hdocdb;

import org.ojai.Document;
import org.ojai.DocumentBuilder;
import org.ojai.Value;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;
import org.ojai.util.Decimals;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;

public class HDocumentBuilder implements DocumentBuilder {

    private HDocument rootDoc = new HDocument();
    private Deque<ContainerState> containers = new ArrayDeque<>();
    private boolean isClosed = false;

    public HDocumentBuilder() {
    }

  /* ===========
   * Map Methods
   * ===========
   */

    /**
     * Associates the specified {@code boolean} value with the specified
     * {@code field} in the current map. Any previous association will be
     * overwritten.
     *
     * @param field the name of the field
     * @param value the {@code boolean} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in a MAP segment
     */
    public HDocumentBuilder put(String field, boolean value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    /**
     * Associates the specified {@code String} value with the specified
     * {@code field} in the current map. Any previous association will be
     * overwritten.
     *
     * @param field the name of the field
     * @param value the {@code String} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in a MAP segment
     */
    public HDocumentBuilder put(String field, String value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, byte value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, short value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, int value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, long value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, float value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, double value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, BigDecimal value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder putDecimal(String field, long decimalValue) {
        getCurrentDocument().set(field, new BigDecimal(decimalValue));
        return this;
    }

    public HDocumentBuilder putDecimal(String field, double decimalValue) {
        getCurrentDocument().set(field, new BigDecimal(decimalValue));
        return this;
    }

    public HDocumentBuilder putDecimal(String field, int unscaledValue, int scale) {
        getCurrentDocument().set(field, Decimals.convertIntToDecimal(unscaledValue, scale));
        return this;
    }

    public HDocumentBuilder putDecimal(String field, long unscaledValue, int scale) {
        getCurrentDocument().set(field, Decimals.convertLongToDecimal(unscaledValue, scale));
        return this;
    }

    public HDocumentBuilder putDecimal(String field, byte[] unscaledValue, int scale) {
        getCurrentDocument().set(field, Decimals.convertByteToBigDecimal(unscaledValue, scale));
        return this;
    }

    public HDocumentBuilder put(String field, byte[] value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, byte[] value, int offset, int length) {
        getCurrentDocument().set(field, ByteBuffer.wrap(value, offset, length));
        return this;
    }

    public HDocumentBuilder put(String field, ByteBuffer value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, ODate value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    /**
     * Associates the specified {@code date} value represented as the number
     * of days since epoch with the specified {@code field} in the
     * current map. Any previous association will be overwritten.
     *
     * @param field the name of the field
     * @param days  the {@code date} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in a MAP segment
     */
    public HDocumentBuilder putDate(String field, int days) {
        getCurrentDocument().set(field, ODate.fromDaysSinceEpoch(days));
        return this;
    }

    public HDocumentBuilder put(String field, OTime value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    /**
     * Associates the specified {@code time} value represented as number of
     * milliseconds since midnight with the specified {@code field} in the
     * current map. Any previous association will be overwritten.
     *
     * @param field  the name of the field
     * @param millis the {@code time} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException    if the builder is not in a MAP segment
     * @throws IllegalArgumentException if the value of {@code millis} is greater
     *                                  than 86400000
     */
    public HDocumentBuilder putTime(String field, int millis) {
        getCurrentDocument().set(field, OTime.fromMillisOfDay(millis));
        return this;
    }

    public HDocumentBuilder put(String field, OTimestamp value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    /**
     * Associates the specified {@code timestamp} value represented as the number
     * of milliseconds since epoch with the specified {@code field} in the
     * current map. Any previous association will be overwritten.
     *
     * @param field      the name of the field
     * @param timeMillis the {@code timestamp} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in a MAP segment
     */
    public HDocumentBuilder putTimestamp(String field, long timeMillis) {
        getCurrentDocument().set(field, new OTimestamp(timeMillis));
        return this;
    }

    public HDocumentBuilder put(String field, OInterval value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder putInterval(String field, long durationInMs) {
        getCurrentDocument().set(field, new OInterval(durationInMs));
        return this;
    }

    public HDocumentBuilder putInterval(String field, int months, int days, int milliseconds) {
        getCurrentDocument().set(field, new OInterval(0, months, days, 0, milliseconds));
        return this;
    }

    public HDocumentBuilder putNewMap(String field) {
        HDocument doc = new HDocument();
        getCurrentDocument().set(field, (Document)doc);
        containers.push(new ContainerState(doc));
        return this;
    }

    public HDocumentBuilder putNewArray(String field) {
        HList list = new HList();
        getCurrentDocument().set(field, (List)list);
        containers.push(new ContainerState(list));
        return this;
    }

    public HDocumentBuilder putNull(String field) {
        getCurrentDocument().set(field, HValue.NULL);
        return this;
    }

    public HDocumentBuilder put(String field, Value value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, Document value) {
        getCurrentDocument().set(field, value);
        return this;
    }

    public HDocumentBuilder put(String field, Map<String, Object> value) {
        getCurrentDocument().set(field, (Document)HValue.initFromMap(value));
        return this;
    }

  /* =============
   * Array Methods
   * =============
   */

    /**
     * Sets the index in the current array at which the next value will be added.
     *
     * @param index the index at which the next value will be added.
     * @return {@code this} for chained invocation
     * @throws IllegalArgumentException if the index is not larger than the last
     *                                  written index.
     * @throws IllegalStateException    if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder setArrayIndex(int index) {
        if (isClosed) {
            throw new IllegalStateException("Builder is closed");
        } else if (containers.peek().container instanceof HList) {
            containers.peek().index = index;
        } else {
            throw new IllegalStateException("Current container is not a list");
        }
        return this;
    }

    private int getAndIncrArrayIndex() {
        if (isClosed) {
            throw new IllegalStateException("Builder is closed");
        } else if (containers.peek().container instanceof HList) {
            int oldIndex = containers.peek().index;
            containers.peek().index = oldIndex + 1;
            return oldIndex;
        } else {
            throw new IllegalStateException("Current container is not a list");
        }
    }

    /**
     * Adds a {@code boolean} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code boolean} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(boolean value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code String} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code String} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(String value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code byte} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code byte} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(byte value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code short} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code short} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(short value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds an {@code int} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code int} value to append
     * @return {@code this} for chained invocation.
     * @throws IllegalStateException if the builder is not in an ARRAY segment.
     */
    public HDocumentBuilder add(int value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code long} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code long} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(long value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code float} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code float} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(float value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code double} value at the current index in the current array and
     * advances the current index by 1.
     *
     * @param value the {@code double} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(double value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a {@code BigDecimal} value at the current index in the current array
     * and advances the current index by 1.
     *
     * @param value the {@code BigDecimal} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(BigDecimal value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Adds a long number as a {@code DECIMAL} value at the current index in the
     * current array and advances the current index by 1.
     *
     * @param decimalValue the {@code long} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder addDecimal(long decimalValue) {
        getCurrentList().set(getAndIncrArrayIndex(), Decimals.convertLongToDecimal(decimalValue, 0));
        return this;
    }

    /**
     * Adds a double number as a {@code DECIMAL} value at the current index in the
     * current array and advances the current index by 1.
     *
     * @param decimalValue the {@code double} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder addDecimal(double decimalValue) {
        getCurrentList().set(getAndIncrArrayIndex(), new BigDecimal(decimalValue));
        return this;
    }

    /**
     * Adds an {@code int} unscaled value and an {@code int} scale as a
     * {@code DECIMAL} value at the current index in the current array
     * and advances the current index by 1. The {@code DECIMAL} value is
     * <tt>(unscaledValue &times; 10<sup>-scale</sup>)</tt>.
     *
     * @param unscaledValue unscaled value of the {@code DECIMAL}
     * @param scale         scale of the {@code DECIMAL}
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder addDecimal(int unscaledValue, int scale) {
        getCurrentList().set(getAndIncrArrayIndex(), Decimals.convertIntToDecimal(unscaledValue, scale));
        return this;
    }

    /**
     * Adds an {@code long} unscaled value and an {@code int} scale as a
     * {@code DECIMAL} value at the current index in the current array
     * and advances the current index by 1. The {@code DECIMAL} value is
     * <tt>(unscaledValue &times; 10<sup>-scale</sup>)</tt>.
     *
     * @param unscaledValue unscaled value of the {@code DECIMAL}
     * @param scale         scale of the {@code DECIMAL}
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder addDecimal(long unscaledValue, int scale) {
        getCurrentList().set(getAndIncrArrayIndex(), Decimals.convertLongToDecimal(unscaledValue, scale));
        return this;
    }

    /**
     * Adds a byte array containing the two's complement binary representation
     * and an {@code int} scale as a {@code DECIMAL} value at the current index
     * in the current array and advances the current index by 1. The input array
     * is assumed to be in <i>big-endian</i> byte-order: the most significant
     * byte is in the zeroth element.
     *
     * @param unscaledValue unscaled value of the {@code DECIMAL}
     * @param scale         scale of the {@code DECIMAL}
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder addDecimal(byte[] unscaledValue, int scale) {
        getCurrentList().set(getAndIncrArrayIndex(), Decimals.convertByteToBigDecimal(unscaledValue, scale));
        return this;
    }

    /**
     * Appends the byte array as a {@code BINARY} value to the current array.
     *
     * @param value the byte array to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(byte[] value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Appends the byte array bounded by offset and length as a {@code BINARY}
     * value to the current array.
     *
     * @param value  the byte array to append
     * @param offset the start offset in the byte array
     * @param length the length from the offset
     * @return {@code this} for chained invocation
     * @throws IllegalStateException     if the builder is not in an ARRAY segment
     * @throws IndexOutOfBoundsException if the offset or offset+length are outside
     *                                   of byte array range
     */
    public HDocumentBuilder add(byte[] value, int offset, int length) {
        getCurrentList().set(getAndIncrArrayIndex(), ByteBuffer.wrap(value, offset, length));
        return this;
    }

    /**
     * Appends the {@code ByteBuffer} as a {@code BINARY} value to the current array.
     *
     * @param value the {@code ByteBuffer} to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(ByteBuffer value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Appends a {@code NULL} value to the current array.
     *
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder addNull() {
        getCurrentList().set(getAndIncrArrayIndex(), HValue.NULL);
        return this;
    }

    /**
     * Appends the {@code Value} to the current array.
     *
     * @param value the {@code Value} to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(Value value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Appends the {@code Document} to the current array.
     *
     * @param value the {@code Document} to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in an ARRAY segment
     */
    public HDocumentBuilder add(Document value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /* Advanced Array Methods */
    public HDocumentBuilder addNewArray() {
        HList list = new HList();
        getCurrentList().set(getAndIncrArrayIndex(), list);
        containers.push(new ContainerState(list));
        return this;
    }

    public HDocumentBuilder addNewMap() {
        HDocument doc;
        if (containers.isEmpty()) {
            doc = rootDoc;
        } else {
            doc = new HDocument();
            getCurrentList().set(getAndIncrArrayIndex(), doc);
        }
        containers.push(new ContainerState(doc));
        return this;
    }

    public HDocumentBuilder add(OTime value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Appends the specified {@code time} value represented as number of
     * milliseconds since midnight to the current array.
     *
     * @param millis the {@code time} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException    if the builder is not in a ARRAY segment
     * @throws IllegalArgumentException if the value of {@code millis} is greater
     *                                  than 86400000
     */
    public HDocumentBuilder addTime(int millis) {
        getCurrentList().set(getAndIncrArrayIndex(), OTime.fromMillisOfDay(millis));
        return this;
    }

    public HDocumentBuilder add(ODate value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    /**
     * Appends the specified {@code date} value represented as the number of
     * days since epoch to the current array.
     *
     * @param days the {@code date} value to append
     * @return {@code this} for chained invocation
     * @throws IllegalStateException if the builder is not in a ARRAY segment
     */
    public HDocumentBuilder addDate(int days) {
        getCurrentList().set(getAndIncrArrayIndex(), ODate.fromDaysSinceEpoch(days));
        return this;
    }

    public HDocumentBuilder add(OTimestamp value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    public HDocumentBuilder addTimestamp(long timeMillis) {
        getCurrentList().set(getAndIncrArrayIndex(), new OTimestamp(timeMillis));
        return this;
    }

    public HDocumentBuilder add(OInterval value) {
        getCurrentList().set(getAndIncrArrayIndex(), value);
        return this;
    }

    public HDocumentBuilder addInterval(long durationInMs) {
        getCurrentList().set(getAndIncrArrayIndex(), new OInterval(durationInMs));
        return this;
    }

    /* Lifecycle methods */
    public HDocumentBuilder endArray() {
        getCurrentList();
        containers.pop();
        return this;
    }

    public HDocumentBuilder endMap() {
        getCurrentDocument();
        containers.pop();
        if (containers.isEmpty()) {
            isClosed = true;
        }
        return this;
    }

    public Document getDocument() {
        if (!isClosed) {
            throw new IllegalStateException("Builder is not closed");
        } else {
            return rootDoc;
        }
    }


    private HDocument getCurrentDocument() {
        if (isClosed) {
            throw new IllegalStateException("Builder is closed");
        } else if (containers.peek().container instanceof HDocument) {
            return (HDocument)containers.peek().container;
        } else {
            throw new IllegalStateException("Current container is not a document");
        }
    }

    private HList getCurrentList() {
        if (isClosed) {
            throw new IllegalStateException("Builder is closed");
        } else if (containers.peek().container instanceof HList) {
            return (HList)containers.peek().container;
        } else {
            throw new IllegalStateException("Current container is not a list");
        }
    }

    static class ContainerState {
        HContainer container;
        int index;

        ContainerState(HContainer container) {
            this.container = container;
            this.index = 0;
        }
    }
}
