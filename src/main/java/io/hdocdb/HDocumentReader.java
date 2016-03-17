package io.hdocdb;

import org.ojai.DocumentReader;
import org.ojai.Value.Type;
import org.ojai.exceptions.TypeException;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;
import org.ojai.util.Types;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.ListIterator;
import java.util.Map.Entry;

class HDocumentReader implements DocumentReader {

    private Deque<IteratorWithType> stateStack = null;
    private IteratorWithType currentItr = null;
    private EventType nextEvent = null;
    private EventType currentEvent = null;
    private String key;
    private HValue value;

    HDocumentReader(HValue value) {
        stateStack = new ArrayDeque<>();
        this.key = null;
        this.value = value;
        Type type = value.getType();
        nextEvent = Types.getEventTypeForType(type);
        if (!type.isScalar()) {
            stateStack.push(new IteratorWithType(value));
        }
    }

    @SuppressWarnings("unchecked")
    private void processNextNode() {
        if (stateStack.isEmpty()) {
            nextEvent = null;
            return;
        }

        currentItr = stateStack.peek();
        if (currentItr.hasNext()) {
            Object o = currentItr.next();
            if (inMap()) {
                Entry<String, HValue> entry = (Entry<String, HValue>)o;
                key = entry.getKey();
                value = entry.getValue();
            } else {
                key = null;
                value = HValue.initFromObject(o);
            }
            nextEvent = Types.getEventTypeForType(value.getType());
            if (!value.getType().isScalar()) {
                stateStack.push(new IteratorWithType(value));
            }
        } else {
            IteratorWithType iter = stateStack.pop();
            key = null;
            value = iter.getValue();
            nextEvent = (iter.getType() == Type.MAP) ? EventType.END_MAP : EventType.END_ARRAY;
            currentItr = stateStack.isEmpty() ? null : stateStack.peek();
        }
    }

    @Override
    public EventType next() {
        currentEvent = null;
        if (nextEvent != null) {
            currentEvent = nextEvent;
            nextEvent = null;
        } else {
            processNextNode();
            currentEvent = nextEvent;
            nextEvent = null;
        }
        return currentEvent;
    }

    private void checkEventType(EventType event) throws TypeException {
        if (currentEvent != event) {
            throw new TypeException(String.format(
                    "Event type mismatch. The operation requires %s, but found %s",
                    event, currentEvent));
        }
    }

    @Override
    public boolean inMap() {
        return currentItr == null
                || currentItr.getType() == Type.MAP;
    }

    @Override
    public int getArrayIndex() {
        if (inMap()) {
            throw new IllegalStateException("Not traversing an array!");
        }
        return currentItr.previousIndex();
    }

    @Override
    public String getFieldName() {
        if (!inMap()) {
            throw new IllegalStateException("Not traversing a map!");
        }
        return key;
    }

    @Override
    public byte getByte() {
        checkEventType(EventType.BYTE);
        return value.getByte();
    }

    @Override
    public short getShort() {
        checkEventType(EventType.SHORT);
        return value.getShort();
    }

    @Override
    public int getInt() {
        checkEventType(EventType.INT);
        return value.getInt();
    }

    @Override
    public long getLong() {
        checkEventType(EventType.LONG);
        return value.getLong();
    }

    @Override
    public float getFloat() {
        checkEventType(EventType.FLOAT);
        return value.getFloat();
    }

    @Override
    public double getDouble() {
        checkEventType(EventType.DOUBLE);
        return value.getDouble();
    }

    @Override
    public BigDecimal getDecimal() {
        checkEventType(EventType.DECIMAL);
        return value.getDecimal();
    }

    @Override
    public int getDecimalPrecision() {
        BigDecimal d = getDecimal();
        if (d != null) {
            return d.precision();
        }
        return 0;
    }

    @Override
    public int getDecimalScale() {
        BigDecimal d = getDecimal();
        if (d != null) {
            return d.scale();
        }
        return 0;
    }

    @Override
    public int getDecimalValueAsInt() {
        BigDecimal d = getDecimal();
        if (d != null) {
            return d.intValueExact();
        }
        return 0;
    }

    @Override
    public long getDecimalValueAsLong() {
        BigDecimal d = getDecimal();
        if (d != null) {
            return d.longValueExact();
        }
        return 0;
    }

    @Override
    public ByteBuffer getDecimalValueAsBytes() {
        BigDecimal decimal = getDecimal();
        if (decimal != null) {
            BigInteger decimalInteger = decimal.unscaledValue();
            byte[] bytearray = decimalInteger.toByteArray();
            return ByteBuffer.wrap(bytearray);
        }
        return null;
    }

    @Override
    public boolean getBoolean() {
        checkEventType(EventType.BOOLEAN);
        return value.getBoolean();
    }

    @Override
    public String getString() {
        checkEventType(EventType.STRING);
        return value.getString();
    }

    @Override
    public long getTimestampLong() {
        checkEventType(EventType.TIMESTAMP);
        return value.getTimestampAsLong();
    }

    @Override
    public OTimestamp getTimestamp() {
        checkEventType(EventType.TIMESTAMP);
        return value.getTimestamp();
    }

    @Override
    public int getDateInt() {
        checkEventType(EventType.DATE);
        return value.getDateAsInt();
    }

    @Override
    public ODate getDate() {
        checkEventType(EventType.DATE);
        return value.getDate();
    }

    @Override
    public int getTimeInt() {
        checkEventType(EventType.TIME);
        return value.getTimeAsInt();
    }

    @Override
    public OTime getTime() {
        checkEventType(EventType.TIME);
        return value.getTime();
    }

    @Override
    public OInterval getInterval() {
        checkEventType(EventType.INTERVAL);
        return value.getInterval();
    }

    @Override
    public int getIntervalDays() {
        return getInterval().getDays();
    }

    @Override
    public long getIntervalMillis() {
        return getInterval().getTimeInMillis();
    }

    @Override
    public ByteBuffer getBinary() {
        checkEventType(EventType.BINARY);
        return value.getBinary();
    }

    private static class IteratorWithType implements ListIterator<Object> {
        final Iterator<?> i;
        final HValue value;

        IteratorWithType(HValue value) {
            this.value = value;
            this.i = (value.getType() == Type.MAP)
                    ? ((HDocument) value).iterator()
                    : ((HList) value).listIterator();
        }

        public HValue getValue() {
            return value;
        }

        @Override
        public boolean hasNext() {
            return i.hasNext();
        }

        @Override
        public Object next() {
            return i.next();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String toString() {
            return (getType() == Type.ARRAY ? "ListIterator@" : "MapIterator@")
                    + hashCode();
        }

        Type getType() {
            return value.getType();
        }

        @Override
        public boolean hasPrevious() {
            checkList();
            return ((ListIterator<?>) i).hasPrevious();
        }

        @Override
        public Object previous() {
            checkList();
            return ((ListIterator<?>) i).previous();
        }

        @Override
        public int nextIndex() {
            checkList();
            return ((ListIterator<?>) i).nextIndex();
        }

        @Override
        public int previousIndex() {
            checkList();
            return ((ListIterator<?>) i).previousIndex();
        }

        @Override
        public void set(Object e) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add(Object e) {
            throw new UnsupportedOperationException();
        }

        private void checkList() {
            if (getType() != Type.ARRAY) {
                throw new UnsupportedOperationException();
            }
        }
    }

}
