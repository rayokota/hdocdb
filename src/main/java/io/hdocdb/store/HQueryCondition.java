package io.hdocdb.store;

import io.hdocdb.HValue;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.Value.Type;
import org.ojai.exceptions.TypeException;
import org.ojai.store.QueryCondition;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.PatternSyntaxException;

public class HQueryCondition implements QueryCondition {

    private ConditionNode root;
    private Deque<ConditionParent> blocks = new ArrayDeque<>();
    private boolean built = false;

    public HQueryCondition() {
    }

    public HQueryCondition(ConditionNode root) {
        this.root = root;
    }

    public HQueryCondition(org.graalvm.polyglot.Value json) {
        and();  // start an implicit and block
        processJson(json);
        close();
    }

    protected void processJson(org.graalvm.polyglot.Value json) {
        for (String key : json.getMemberKeys()) {
            org.graalvm.polyglot.Value value = json.getMember(key);
            if (key.startsWith("$")) {
                switch (key) {
                    case "$and":
                        and();
                        processJsonList(value);
                        close();
                        break;
                    case "$or":
                        or();
                        processJsonList(value);
                        close();
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal operator: " + key);
                }
            } else {
                FieldPath path = FieldPath.parseFrom(key);
                if (value.hasMembers()) {
                    processJsonMap(path, value);
                } else {
                    add(new ConditionLeaf(path,
                            ConditionLeaf.CompareOp.EQ,
                            HValue.initFromObject(value)));
                }
            }
        }
    }

    protected void processJsonList(org.graalvm.polyglot.Value json) {
        for (int i = 0; i < json.getArraySize(); i++) {
            processJson(json.getArrayElement(i));
        }
    }

    protected void processJsonMap(FieldPath path, org.graalvm.polyglot.Value json) {
        if (isLiteral(json)) {
            add(new ConditionLeaf(path,
                    ConditionLeaf.CompareOp.EQ,
                    HValue.initFromObject(json)));
        } else {
            for (String key : json.getMemberKeys()) {
                add(new ConditionLeaf(path,
                        processJsonCondition(key),
                        HValue.initFromObject(json.getMember(key))));
            }
        }
    }

    private boolean isLiteral(org.graalvm.polyglot.Value json) {
        Iterator<String> keySet = json.getMemberKeys().iterator();
        String firstKey = keySet.hasNext() ? keySet.next() : null;
        return firstKey != null && !firstKey.startsWith("$");
    }

    protected ConditionLeaf.CompareOp processJsonCondition(String op) {
        switch (op) {
            case "$lt":
                return ConditionLeaf.CompareOp.LT;
            case "$lte":
                return ConditionLeaf.CompareOp.LE;
            case "$eq":
                return ConditionLeaf.CompareOp.EQ;
            case "$ne":
                return ConditionLeaf.CompareOp.NE;
            case "$gte":
                return ConditionLeaf.CompareOp.GE;
            case "$gt":
                return ConditionLeaf.CompareOp.GT;
            case "$in":
                return ConditionLeaf.CompareOp.IN;
            case "$nin":
                return ConditionLeaf.CompareOp.NOT_IN;
            default:
                throw new IllegalArgumentException("Illegal operator: " + op);
        }
    }

    public ConditionNode getRoot() {
        return root;
    }

    /**
     * @return {@code true} if this condition is empty
     */
    public boolean isEmpty() {
        return root == null;
    }

    /**
     * @return {@code true} if this condition is built
     */
    public boolean isBuilt() {
        return built;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HQueryCondition that = (HQueryCondition) o;

        return !(!Objects.equals(root, that.root));
    }

    @Override
    public int hashCode() {
        return root != null ? root.hashCode() : 0;
    }

    public String toString() {
        return root != null ? root.toString() : "";
    }

    /**
     * Begins a new AND compound condition block.
     *
     * @return {@code this} for chaining
     */
    public HQueryCondition and() {
        return add(new ConditionParent(ConditionParent.BooleanOp.AND));
    }

    /**
     * Begins a new OR compound condition block.
     *
     * @return {@code this} for chaining
     */
    public HQueryCondition or() {
        return add(new ConditionParent(ConditionParent.BooleanOp.OR));
    }

    /**
     * Begins a new element-wise AND compound condition block.
     * @param fieldPath the array expression for the fields' container
     * @return {@code this} for chaining
     */
    public QueryCondition elementAnd(String fieldPath) {
        throw new UnsupportedOperationException();
    }

    /**
     * Begins a new element-wise AND compound condition block.
     * @param fieldPath the array expression for the fields' container
     * @return {@code this} for chaining
     */
    public QueryCondition elementAnd(FieldPath fieldPath) {
        throw new UnsupportedOperationException();
    }

    /**
     * Closes a compound condition block.
     *
     * @return {@code this} for chaining
     */
    public HQueryCondition close() {
        if (!blocks.isEmpty()) {
            ConditionParent block = blocks.pop();
            block.close();
        }
        return this;
    }

    /**
     * Closes all nested compound condition blocks.
     *
     * @return {@code this}
     */
    public HQueryCondition build() {
        if (built) {
            throw new IllegalStateException("Condition is already built");
        }
        if (!blocks.isEmpty()) {
            throw new IllegalStateException("Missing call to close");
        }

        built = true;
        return this;
    }

    /**
     * Appends the specified condition to the current condition
     * block.
     *
     * @return {@code this} for chaining
     */
    public HQueryCondition condition(QueryCondition conditionToAdd) {
        if (conditionToAdd == null) throw new IllegalArgumentException("condition is null");
        if (conditionToAdd == this) throw new IllegalArgumentException("condition is this");
        if (!conditionToAdd.isBuilt()) throw new IllegalArgumentException("condition is unbuilt");
        if (conditionToAdd.isEmpty()) throw new IllegalArgumentException("condition is empty");

        ConditionNode root = ((HQueryCondition) conditionToAdd).getRoot().deepCopy();
        if (this.root == null) {
            this.root = root;
        } else {
            blocks.peek().add(root);
        }

        return this;
    }

    /**
     * Adds a condition that tests for existence of the specified
     * {@code FieldPath}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition exists(String path) {
        return exists(FieldPath.parseFrom(path));
    }

    /**
     * Adds a condition that tests for existence of the specified
     * {@code FieldPath}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition exists(FieldPath path) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NE, HValue.NULL));
    }

    /**
     * Adds a condition that tests for non-existence of the specified
     * {@code FieldPath}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notExists(String path) {
        return notExists(FieldPath.parseFrom(path));
    }

    /**
     * Adds a condition that tests for non-existence of the specified
     * {@code FieldPath}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notExists(FieldPath path) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.EQ, HValue.NULL));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is equal to at least one of the values in the
     * specified {@code List}.
     *
     * @param path        the {@code FieldPath} to test
     * @param listOfValue the {@code List} of values to test against
     * @return {@code this} for chained invocation
     */
    public HQueryCondition in(String path, List<? extends Object> listOfValue) {
        return in(FieldPath.parseFrom(path), listOfValue);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is equal to at least one of the values in the
     * specified {@code List}.
     *
     * @param path        the {@code FieldPath} to test
     * @param listOfValue the {@code List} of values to test against
     * @return {@code this} for chained invocation
     */
    public HQueryCondition in(FieldPath path, List<? extends Object> listOfValue) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.IN, HValue.initFromList(listOfValue)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is not equal to any of the values in the
     * specified {@code List}.
     *
     * @param path        the {@code FieldPath} to test
     * @param listOfValue the {@code List} of values to test against
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notIn(String path, List<? extends Object> listOfValue) {
        return notIn(FieldPath.parseFrom(path), listOfValue);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is not equal to any of the values in the
     * specified {@code List}.
     *
     * @param path        the {@code FieldPath} to test
     * @param listOfValue the {@code List} of values to test against
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notIn(FieldPath path, List<? extends Object> listOfValue) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NOT_IN, HValue.initFromList(listOfValue)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is of the specified {@code Type}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition typeOf(String path, Type type) {
        return typeOf(FieldPath.parseFrom(path), type);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is of the specified {@code Type}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition typeOf(FieldPath path, Type type) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.TYPE_OF, type));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is not of the specified {@code Type}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notTypeOf(String path, Type type) {
        return notTypeOf(FieldPath.parseFrom(path), type);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is not of the specified {@code Type}.
     *
     * @param path the {@code FieldPath} to test
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notTypeOf(FieldPath path, Type type) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NOT_TYPE_OF, type));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and matches the specified regular
     * expression.
     *
     * @param path  the {@code FieldPath} to test
     * @param regex the reference regular expression
     * @return {@code this} for chained invocation
     * @throws PatternSyntaxException if the expression's syntax is invalid
     */
    public HQueryCondition matches(String path, String regex) {
        return matches(FieldPath.parseFrom(path), regex);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and matches the specified regular
     * expression.
     *
     * @param path  the {@code FieldPath} to test
     * @param regex the reference regular expression
     * @return {@code this} for chained invocation
     * @throws PatternSyntaxException if the expression's syntax is invalid
     */
    public HQueryCondition matches(FieldPath path, String regex) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.MATCHES, new HValue(regex)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and does not match the specified
     * regular expression.
     *
     * @param path  the {@code FieldPath} to test
     * @param regex the reference regular expression
     * @return {@code this} for chained invocation
     * @throws PatternSyntaxException if the expression's syntax is invalid
     */
    public HQueryCondition notMatches(String path, String regex) {
        return notMatches(FieldPath.parseFrom(path), regex);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and does not match the specified
     * regular expression.
     *
     * @param path  the {@code FieldPath} to test
     * @param regex the reference regular expression
     * @return {@code this} for chained invocation
     * @throws PatternSyntaxException if the expression's syntax is invalid
     */
    public HQueryCondition notMatches(FieldPath path, String regex) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NOT_MATCHES, new HValue(regex)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and matches the specified SQL LIKE
     * expression.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition like(String path, String likeExpression) {
        return like(FieldPath.parseFrom(path), likeExpression);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and matches the specified SQL LIKE
     * expression.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition like(FieldPath path, String likeExpression) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.LIKE, new HValue(likeExpression)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and matches the specified SQL LIKE
     * expression optionally escaped with the specified escape character.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @param escapeChar     the escape character in the LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition like(String path, String likeExpression, Character escapeChar) {
        return like(FieldPath.parseFrom(path), likeExpression, escapeChar);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and matches the specified SQL LIKE
     * expression optionally escaped with the specified escape character.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @param escapeChar     the escape character in the LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition like(FieldPath path, String likeExpression, Character escapeChar) {
        // TODO like
        throw new UnsupportedOperationException();
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and does not match the specified
     * SQL LIKE expression.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notLike(String path, String likeExpression) {
        return notLike(FieldPath.parseFrom(path), likeExpression);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and does not match the specified
     * SQL LIKE expression.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notLike(FieldPath path, String likeExpression) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NOT_LIKE, new HValue(likeExpression)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and does not match the specified
     * SQL LIKE expression optionally escaped with the specified escape character.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @param escapeChar     the escape character in the LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notLike(String path, String likeExpression, Character escapeChar) {
        return notLike(FieldPath.parseFrom(path), likeExpression, escapeChar);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} is a String and does not match the specified
     * SQL LIKE expression optionally escaped with the specified escape character.
     *
     * @param path           the {@code FieldPath} to test
     * @param likeExpression the reference LIKE pattern
     * @param escapeChar     the escape character in the LIKE pattern
     * @return {@code this} for chained invocation
     */
    public HQueryCondition notLike(FieldPath path, String likeExpression, Character escapeChar) {
        // TODO notLike
        throw new UnsupportedOperationException();
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code boolean} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference boolean {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, boolean value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code boolean} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference boolean {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, boolean value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code String} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference String {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, String value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code String} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference String {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, String value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code byte} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference byte {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, byte value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code byte} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference byte {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, byte value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code short} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference short {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, short value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code short} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference short {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, short value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code int} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference int {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, int value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code int} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference int {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, int value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code long} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference long {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, long value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code long} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference long {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, long value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code float} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference float {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, float value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code float} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference float {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, float value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code double} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference double {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, double value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code double} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference double {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, double value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code BigDecimal} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference BigDecimal {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, BigDecimal value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code BigDecimal} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference BigDecimal {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, BigDecimal value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Date} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Date {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, ODate value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Date} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Date {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, ODate value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Time} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Time {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, OTime value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Time} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Time {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, OTime value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Timestamp} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Timestamp {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, OTimestamp value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Timestamp} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Timestamp {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, OTimestamp value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Interval} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Interval {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, OInterval value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Interval} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference Interval {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, OInterval value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code ByteBuffer} value. Only the byte sequence between
     * the {@code position} and {@code limit} in the {@code ByteBuffer} is
     * used as the reference value. The function does not alter the passed
     * ByteBuffer state or content.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference ByteBuffer {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, ByteBuffer value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code ByteBuffer} value. Only the byte sequence between
     * the {@code position} and {@code limit} in the {@code ByteBuffer} is
     * used as the reference value. The function does not alter the passed
     * ByteBuffer state or content.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference ByteBuffer {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, ByteBuffer value) {
        return add(new ConditionLeaf(path, asOpType(op), new HValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Value} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(String path, Op op, Value value) {
        return is(FieldPath.parseFrom(path), op, value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} satisfies the given {@link Op} against
     * the specified {@code Value} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param op    the {@code QueryCondition.Op} to apply
     * @param value the reference {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition is(FieldPath path, Op op, Value value) {
        return add(new ConditionLeaf(path, asOpType(op), HValue.initFromValue(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} equals the specified {@code Map} value.
     * Two Maps are considered equal if and only if they contain the same
     * key-value pair in the same order.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference Map {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value at any level in the specified
     *                       Map is not one of the {@code Value} types
     */
    public HQueryCondition equals(String path, Map<String, ? extends Object> value) {
        return equals(FieldPath.parseFrom(path), value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} equals the specified {@code Map} value.
     * Two Maps are considered equal if and only if they contain the same
     * key-value pair in the same order.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference Map {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value at any level in the specified
     *                       Map is not one of the {@code Value} types
     */
    public HQueryCondition equals(FieldPath path, Map<String, ? extends Object> value) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.EQ, HValue.initFromMap(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} equals the specified {@code List} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference List {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value in the specified List is not one of
     *                       the {@code Value} types
     */
    public HQueryCondition equals(String path, List<? extends Object> value) {
        return equals(FieldPath.parseFrom(path), value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} equals the specified {@code List} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference List {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value in the specified List is not one of
     *                       the {@code Value} types
     */
    public HQueryCondition equals(FieldPath path, List<? extends Object> value) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.EQ, HValue.initFromList(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} does not equal the specified {@code Map} value.
     * Two Maps are considered equal if and only if they contain the same
     * key-value pair in the same order.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference Map {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value at any level in the specified
     *                       Map is not one of the {@code Value} types
     */
    public HQueryCondition notEquals(String path, Map<String, ? extends Object> value) {
        return notEquals(FieldPath.parseFrom(path), value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} does not equal the specified {@code Map} value.
     * Two Maps are considered equal if and only if they contain the same
     * key-value pair in the same order.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference Map {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value at any level in the specified
     *                       Map is not one of the {@code Value} types
     */
    public HQueryCondition notEquals(FieldPath path, Map<String, ? extends Object> value) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NE, HValue.initFromMap(value)));
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} does not equal the specified {@code List} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference List {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value in the specified List is not one of
     *                       the {@code Value} types
     */
    public HQueryCondition notEquals(String path, List<? extends Object> value) {
        return notEquals(FieldPath.parseFrom(path), value);
    }

    /**
     * Adds a condition that tests if the {@code Value} at the specified
     * {@code FieldPath} does not equal the specified {@code List} value.
     *
     * @param path  the {@code FieldPath} to test
     * @param value the reference List {@code Value}
     * @return {@code this} for chained invocation
     * @throws TypeException if a value in the specified List is not one of
     *                       the {@code Value} types
     */
    public HQueryCondition notEquals(FieldPath path, List<? extends Object> value) {
        return add(new ConditionLeaf(path, ConditionLeaf.CompareOp.NE, HValue.initFromList(value)));
    }

    /**
     * Adds a condition that tests if the size of the {@code Value} at the
     * specified {@code FieldPath} satisfies the given {@link Op} and the size.
     * The value must be one of the following types: {@code STRING},
     * {@code BINARY}, {@code MAP} or {@code ARRAY}.
     *
     * @param path the {@code FieldPath} to test
     * @param op   the {@code QueryCondition.Op} to apply
     * @param size the reference size of {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition sizeOf(String path, Op op, long size) {
        return sizeOf(FieldPath.parseFrom(path), op, size);
    }

    /**
     * Adds a condition that tests if the size of the {@code Value} at the
     * specified {@code FieldPath} satisfies the given {@link Op} and the size.
     * The value must be one of the following types: {@code STRING},
     * {@code BINARY}, {@code MAP} or {@code ARRAY}.
     *
     * @param path the {@code FieldPath} to test
     * @param op   the {@code QueryCondition.Op} to apply
     * @param size the reference size of {@code Value}
     * @return {@code this} for chained invocation
     */
    public HQueryCondition sizeOf(FieldPath path, Op op, long size) {
        throw new UnsupportedOperationException();
    }

    public Set<FieldPath> getConditionPaths() {
        return root.getConditionPaths();
    }

    public Map<FieldPath, ConditionRange> getConditionRanges() throws IllegalStateException {
        return root.getConditionRanges();
    }

    public boolean evaluate(Document document) {
        return root.evaluate(document);
    }

    private HQueryCondition add(ConditionLeaf leaf) {
        if (this.root == null) {
            this.root = leaf;
        } else {
            blocks.peek().add(leaf);
        }
        return this;
    }

    private HQueryCondition add(ConditionParent block) {
        if (this.root == null) {
            this.root = block;
        } else {
            blocks.peek().add(block);
        }
        blocks.push(block);
        return this;
    }

    private static ConditionLeaf.CompareOp asOpType(Op op) {
        switch (op) {
            case LESS:
                return ConditionLeaf.CompareOp.LT;
            case LESS_OR_EQUAL:
                return ConditionLeaf.CompareOp.LE;
            case EQUAL:
                return ConditionLeaf.CompareOp.EQ;
            case NOT_EQUAL:
                return ConditionLeaf.CompareOp.NE;
            case GREATER:
                return ConditionLeaf.CompareOp.GT;
            case GREATER_OR_EQUAL:
                return ConditionLeaf.CompareOp.GE;
        }
        throw new IllegalArgumentException("invalid op");
    }
}
