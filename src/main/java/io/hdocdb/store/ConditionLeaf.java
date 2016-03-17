package io.hdocdb.store;

import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import io.hdocdb.HList;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.Value.Type;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Map;
import java.util.Set;

public class ConditionLeaf extends ConditionNode {

    public enum CompareOp {
        NONE(0),
        LT(1),
        LE(2),
        EQ(3),
        NE(4),
        GE(5),
        GT(6),
        IN(7),
        NOT_IN(8),
        MATCHES(9),
        NOT_MATCHES(10),
        LIKE(11),
        NOT_LIKE(12),
        TYPE_OF(13),
        NOT_TYPE_OF(14);

        private static final EnumHashBiMap<CompareOp, Integer> lookup = EnumHashBiMap.create(CompareOp.class);
        static {
            for (CompareOp type : CompareOp.values()) {
                lookup.put(type, type.getCode());
            }
        }

        private final int code;
        CompareOp(int code) {
            this.code = code;
        }
        public int getCode() {
            return code;
        }

        public static CompareOp valueOf(int value) {
            return lookup.inverse().get(value);
        }

        public String toString() {
            switch (this) {
                case LT:
                    return "<";
                case LE:
                    return "<=";
                case EQ:
                    return "=";
                case NE:
                    return "!=";
                case GE:
                    return ">=";
                case GT:
                    return ">";
                default:
                    return super.toString();
            }
        }
    }

    private FieldPath field;
    private CompareOp op;
    private HValue value;
    private Type type;

    public ConditionLeaf() {
        this(FieldPath.EMPTY, CompareOp.NONE, HValue.NULL);
    }

    public ConditionLeaf(FieldPath field, CompareOp op, HValue value) {
        this.field = field;
        this.op = op;
        this.value = value;
        this.type = null;
    }

    public ConditionLeaf(FieldPath field, CompareOp op, Type type) {
        this.op = op;
        this.field = field;
        this.value = null;
        this.type = type;
    }

    public ConditionNode deepCopy() {
        return value != null ? new ConditionLeaf(field, op, value) : new ConditionLeaf(field, op, type);
    }

    public boolean isEmpty() {
        return false;
    }

    public boolean isLeaf() {
        return true;
    }

    public FieldPath getField() {
        return this.field;
    }

    public CompareOp getOp() {
        return this.op;
    }

    public HValue getValue() {
        return this.value;
    }

    public Type getType() {
        return this.type;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        sb.append(this.field.asPathString());
        sb.append(' ');
        sb.append(this.op);
        sb.append(' ');
        sb.append(this.value != null ? this.value : this.type);
        sb.append(')');
        return sb.toString();
    }

    public Set<FieldPath> getConditionPaths() {
        return ImmutableSet.of(getField());
    }

    public Map<FieldPath, ConditionRange> getConditionRanges() {
        Range<HValue> range = getRange();
        if (range == null) throw new IllegalStateException("Could not obtain valid range for index scan");
        return ImmutableMap.of(getField(), new ConditionRange(getField(), getRange(), this));
    }

    /*
     * Returns Range.all() for a full index scan, returns null for a full table scan.
     * Must return null if otherwise the answer would need to distinguish between Type.NULL
     * and a value of the wrong type (which is also encoded as Type.NULL in the index)
     */
    public Range<HValue> getRange() {
        // for now don't support index scans using null
        if (value.getType() == Type.NULL) return null;
        switch (op) {
            case NONE:
                return null;
            case LT:
                return Range.lessThan(value);
            case LE:
                return Range.atMost(value);
            case EQ:
                return Range.singleton(value);
            case NE:
                return Range.all();
            case GE:
                return Range.atLeast(value);
            case GT:
                return Range.greaterThan(value);
            case IN:
                return Range.encloseAll(((HList) getValue().getList()).getHValues());
            case NOT_IN:
                return Range.all();  // need to scan all
            case MATCHES:
            case NOT_MATCHES:
                return Range.all();
            case LIKE:
            case NOT_LIKE:
                return Range.all();
            case TYPE_OF:
            case NOT_TYPE_OF:
                return null;
            default:
                throw new IllegalArgumentException("Illegal op " + op);
        }
    }

    public boolean evaluate(Document document) {
        CompareOp op = getOp();
        HValue value = document != null ? HValue.initFromValue(document.getValue(getField())) : HValue.NULL;
        Value.Type valueType = value != null ? value.getType() : Type.NULL;
        // handle []
        if (valueType == Type.ARRAY) {
            return ((HList) value).evaluate(op, getValue());
        }
        switch (op) {
            case NONE:
                return true;
            case EQ:
            case NE:
                boolean equals = getValue().equals(value);
                return op == CompareOp.EQ && equals || op == CompareOp.NE && !equals;
            case LT:
            case LE:
            case GE:
            case GT:
                if (valueType == Type.NULL) return false;
                try {
                    int compare = value.compareTo(getValue());
                    return op == CompareOp.LT && compare < 0
                            || op == CompareOp.LE && compare <= 0
                            || op == CompareOp.GE && compare >= 0
                            || op == CompareOp.GT && compare > 0;
                } catch (IllegalArgumentException e) {
                    return false;
                }
            case IN:
            case NOT_IN:
                boolean in = getValue().getList().contains(value);
                return op == CompareOp.IN && in || op == CompareOp.NOT_IN && !in;
            case MATCHES:
            case NOT_MATCHES:
                // TODO regex
                throw new UnsupportedOperationException();
            case LIKE:
            case NOT_LIKE:
                // TODO like
                throw new UnsupportedOperationException();
            case TYPE_OF:
                return valueType == getType();
            case NOT_TYPE_OF:
                return valueType != getType();
            default:
                throw new IllegalArgumentException("Illegal op " + op);
        }
    }

    public void readExternal(ObjectInput input) throws IOException {
        String path = input.readUTF();
        int opType = input.readInt();
        boolean hasValue = input.readByte() == 1;
        HValue value = null;
        int typeCode = 0;
        if (hasValue) {
            HValueHolder valueHolder = new HValueHolder();
            valueHolder.readExternal(input);
            value = valueHolder.getValue();
        } else {
            typeCode = input.readInt();
        }
        this.field = FieldPath.parseFrom(path);
        this.op = CompareOp.valueOf(opType);
        this.value = value;
        this.type = Type.valueOf(typeCode);
    }

    public void writeExternal(ObjectOutput output) throws IOException {
        output.writeUTF(field.asPathString());
        output.writeInt(op.getCode());
        output.writeByte(value != null ? 1 : 0);
        if (value != null) {
            new HValueHolder(value).writeExternal(output);
        } else {
            output.writeInt(type.getCode());
        }
    }

}
