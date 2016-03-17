package io.hdocdb.store;

import com.google.common.collect.EnumHashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.ojai.Document;
import org.ojai.FieldPath;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConditionParent extends ConditionNode {

    public enum BooleanOp {
        AND(1),
        OR(2);

        private static final EnumHashBiMap<BooleanOp, Integer> lookup = EnumHashBiMap.create(BooleanOp.class);
        static {
            for (BooleanOp type : BooleanOp.values()) {
                lookup.put(type, type.getCode());
            }
        }

        private final int code;
        BooleanOp(int code) {
            this.code = code;
        }
        public int getCode() {
            return code;
        }

        public static BooleanOp valueOf(int value) {
            return lookup.inverse().get(value);
        }
    }

    private BooleanOp type;
    private List<ConditionNode> children = Lists.newArrayList();
    private transient boolean closed = false;

    public ConditionParent() {
        this.type = BooleanOp.AND;
    }

    public ConditionParent(BooleanOp type) {
        this.type = type;
    }

    public ConditionNode deepCopy() {
        ConditionParent node = new ConditionParent(type);
        for (ConditionNode child : getChildren()) {
            node.add(child.deepCopy());
        }
        return node;
    }

    public boolean isEmpty() {
        return children.isEmpty();
    }

    public boolean isLeaf() {
        return false;
    }

    public BooleanOp getType() {
        return this.type;
    }

    public List<ConditionNode> getChildren() {
        return this.children;
    }

    public void add(ConditionNode node) {
        children.add(node);
    }

    public void addAll(List<? extends ConditionNode> nodes) {
        children.addAll(nodes);
    }

    public void close() {
        this.closed = true;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        boolean first = true;
        for (ConditionNode child : children) {
            if (first) {
                first = false;
            } else {
                sb.append(", ");
            }
            sb.append(child);
        }
        sb.append(')');
        return sb.toString();
    }

    public Set<FieldPath> getConditionPaths() {
        Set<FieldPath> paths = Sets.newHashSet();
        for (ConditionNode child : getChildren()) {
            paths.addAll(child.getConditionPaths());
        }
        return paths;
    }

    public Map<FieldPath, ConditionRange> getConditionRanges() throws IllegalStateException {
        if (getType() == BooleanOp.OR) throw new IllegalStateException("Cannot get bounds for disjunction");
        Map<FieldPath, ConditionRange> bounds = Maps.newHashMap();
        for (ConditionNode child : getChildren()) {
            bounds = mergeConditionRanges(bounds, child.getConditionRanges());
        }
        return bounds;
    }

    private Map<FieldPath, ConditionRange> mergeConditionRanges(Map<FieldPath, ConditionRange> b1, Map<FieldPath, ConditionRange> b2) {
        Map<FieldPath, ConditionRange> result = Maps.newHashMap(b1);
        for (Map.Entry<FieldPath, ConditionRange> entry : b2.entrySet()) {
            FieldPath path = entry.getKey();
            ConditionRange range2 = entry.getValue();
            ConditionRange range1 = b1.get(path);
            result.put(path, range1 != null ? range1.merge(range2) : range2);
        }
        return result;
    }

    public boolean evaluate(Document document) {
        boolean result;
        if (getType() == BooleanOp.AND) {
            result = true;
            for (ConditionNode node : getChildren()) {
                if (!node.evaluate(document)) {
                    result = false;
                    break;
                }
            }
        } else {
            result = false;
            for (ConditionNode node : getChildren()) {
                if (node.evaluate(document)) {
                    result = true;
                    break;
                }
            }
        }
        return result;
    }

    public void readExternal(ObjectInput input) throws IOException, ClassNotFoundException {
        int blockType = input.readInt();
        int numChildren = input.readInt();
        List<ConditionNode> children = Lists.newArrayListWithExpectedSize(numChildren);
        for (int i = 0; i < numChildren; i++) {
            boolean isLeaf = input.readByte() == 1;
            ConditionNode child = isLeaf ? new ConditionLeaf() : new ConditionParent();
            child.readExternal(input);
            children.add(child);
        }
        this.type = BooleanOp.valueOf(blockType);
        this.children = children;
    }

    public void writeExternal(ObjectOutput output) throws IOException {
        output.writeInt(type.getCode());
        output.writeInt(children.size());
        for (ConditionNode child : children) {
            output.writeByte(child.isLeaf() ? 1 : 0);
            child.writeExternal(output);
        }
    }
}
