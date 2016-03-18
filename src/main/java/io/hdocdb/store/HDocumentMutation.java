package io.hdocdb.store;

import com.google.common.collect.ImmutableList;
import io.hdocdb.HDocument;
import io.hdocdb.HList;
import io.hdocdb.HValue;
import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.Value.Type;
import org.ojai.store.DocumentMutation;
import org.ojai.store.MutationOp;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.ODate;
import org.ojai.types.OInterval;
import org.ojai.types.OTime;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The DocumentMutation interface defines the APIs to perform mutation of a
 * Document already stored in a DocumentStore.
 * <br/><br/>
 * Please see the following notes regarding the behavior of the API types in
 * this Interface.
 * <p/>
 * <h3>{@code set()}</h3>
 * These APIs validate the type of existing value at the specified FieldPath
 * before applying the mutation. If the field does not exist in the corresponding
 * Document in the DocumentStore, it is created. If the field exists but is not
 * of the same type as the type of new value, then the entire mutation fails.
 * <p/>
 * <h3>{@code setOrReplace()}</h3>
 * These are performant APIs that do not require or perform a read-modify-write
 * operation on the server.<br/><br/>
 * If a segment in the specified FieldPath doesn't exist, it is created. For example:
 * <blockquote>{@code setOrReplace("a.b.c", (int) 10)}</blockquote>
 * In this example, if the Document stored in the DocumentStore has an empty MAP
 * field {@code "a"}, then a setOrReplace of {@code "a.b.c"} will create a field
 * {@code "b"} of type MAP under {@code "a"}. It will also create an field named
 * {@code "c"} of type INTEGER under {@code "b"} and set its value to 10.<br/><br/>
 * <p/>
 * If any segment specified in the FieldPath is of a different type than the
 * existing field segment on the server, it will be deleted and replaced by
 * a new segment. For example:
 * <blockquote>{@code setOrReplace("a.b.c", (int) 10)}<br/></blockquote>
 * If the Document stored in the DocumentStore has a field "a" of type array.
 * This operation will delete "a", create new field "a" of type map, add a MAP
 * field "b" under "a" and finally create an INTEGER field "c" with value 10 under
 * "b".<br/><br/>
 * <b/>Warning:</b> These are potentially destructive operations since they do
 * not validate existence or type of any field segment in the specified FieldPath.
 * <p/>
 * <h3>{@code append()}</h3>
 * These operations perform read-modify-write on the server and will fail if
 * type of any of the intermediate fields segment in the specified FieldPath
 * does not match the type of the corresponding field in the document stored
 * on server. For example, an append operation on field {@code "a.b.c"} will
 * fail if, on the server, the field {@code "a"} itself is an ARRAY or INTEGER.
 * <p/>
 * <h3>{@code merge()}</h3>
 * If the specified field is of a type other than MAP, then the operation will fail.
 * If the field doesn't exist in the Document on the server, then this operation will
 * create a new field at the given path. This new field will be of the MAP type and
 * its value will be as specified in the parameter.
 * <p/>
 * This operation will fail if any type of intermediate field segment specified
 * in the FieldPath doesn't match the type of the corresponding field in the
 * record stored on the server. For example, a merge operation on field {@code "a.b.c"}
 * will fail if, on the server, the field {@code "a"} itself is an ARRAY or INTEGER.
 * <p/>
 * <h3>{@code increment()}</h3>
 * If the FieldPath specified for the incremental change doesn't exist in the
 * corresponding Document in the DocumentStore then this operation will create
 * a new field at the given path. This new field will be of same type as the
 * value specified in the parameter.
 * <br/><br/>
 * This operation will fail if the type of any intermediate fields specified
 * in the FieldPath doesn't match the type of the corresponding field in the
 * record stored on the server. For example, an operation on field "a.b.c" will fail
 * if, on the server, record a itself is an array or integer.
 * <br/><br/>
 * An increment operation can be applied on any of the numeric types such as byte,
 * short, int, long, float, double, or decimal. The operation will fail if the
 * increment is applied to a field that is of a non-numeric type.
 * <p/>
 * The increment operation won't change the type of the existing value stored in
 * the given field for the row. The resultant value of the field will be
 * truncated based on the original type of the field.
 * <p/>
 * For example, field 'score' is of type int and contains 60. The increment
 * '5.675', a double, is applied. The resultant value of the field will be 65
 * (65.675 will be truncated to 65).
 */
public class HDocumentMutation implements DocumentMutation {

    private List<MutationOp> mutationOps = new ArrayList<>();

    public HDocumentMutation() {
    }

    public HDocumentMutation(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("$")) {
                if (!(entry.getValue() instanceof ScriptObjectMirror)) {
                    throw new IllegalArgumentException("Illegal operator value: " + entry.getValue());
                }
                ScriptObjectMirror value = (ScriptObjectMirror) entry.getValue();
                switch (key) {
                    case "$set":
                        processJsonSet(value);
                        break;
                    case "$unset":
                        processJsonUnset(value);
                        break;
                    case "$inc":
                        processJsonInc(value);
                        break;
                    case "$append":
                        processJsonAppend(value);
                        break;
                    case "$merge":
                        processJsonMerge(value);
                        break;
                    case "$push":
                        processJsonPush(value);
                        break;
                    case "$pushAll":
                        processJsonAppend(value);
                        break;
                    default:
                        throw new IllegalArgumentException("Illegal operator: " + key);
                }
            } else {
                throw new IllegalArgumentException("Mutation must start with operator: " + key);
            }
        }
    }

    protected void processJsonSet(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            setOrReplace(entry.getKey(), HValue.initFromObject(entry.getValue()));
        }
    }

    protected void processJsonUnset(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            delete(entry.getKey());
        }
    }

    protected void processJsonInc(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            if (entry.getValue() instanceof Number) {
                increment(entry.getKey(), (Number) entry.getValue());
            } else {
                throw new IllegalArgumentException("Illegal arg to $inc: " + entry.getValue());
            }
        }
    }

    protected void processJsonAppend(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            HValue value = HValue.initFromObject(entry.getValue());
            if (value.getType() == Type.STRING) {
                append(entry.getKey(), value.getString());
            } else if (value.getType() == Type.ARRAY) {
                append(entry.getKey(), value.getList());
            } else {
                throw new IllegalArgumentException("Illegal arg to $append: " + entry.getValue());
            }
        }
    }

    protected void processJsonPush(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            HValue value = HValue.initFromObject(entry.getValue());
            append(entry.getKey(), ImmutableList.of(value));
        }
    }

    protected void processJsonMerge(ScriptObjectMirror json) {
        for (Map.Entry<String, Object> entry : json.entrySet()) {
            HValue value = HValue.initFromObject(entry.getValue());
            if (value.getType() == Type.MAP) {
                merge(entry.getKey(), value.getMap());
            } else {
                throw new IllegalArgumentException("Illegal arg to $merge: " + entry.getValue());
            }
        }
    }

    /**
     * Empties this Mutation object.
     */
    public HDocumentMutation empty() {
        mutationOps.clear();
        return this;
    }

    public boolean isReadModifyWrite() {
        for (MutationOp op : mutationOps) {
            if (op.getType() == MutationOp.Type.SET || op.getType() == MutationOp.Type.INCREMENT
                || op.getType() == MutationOp.Type.APPEND || op.getType() == MutationOp.Type.MERGE
                // we need to specify delete to allow maps and arrays to be deleted
                || op.getType() == MutationOp.Type.DELETE) {
                return true;
            }
        }
        return false;
    }

    public HDocument asDocument() {
        if (isReadModifyWrite()) return null;
        HDocument document = new HDocument();
        for (MutationOp op : mutationOps) {
            document.set(op.getFieldPath(), op.getOpValue());
        }
        return document;
    }

    /**
     * Sets the field at the given FieldPath to {@link Type#NULL NULL} Value.
     *
     * @param path path of the field that needs to be updated
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setNull(String path) {
        return setNull(FieldPath.parseFrom(path));
    }

    /**
     * Sets the field at the given FieldPath to {@link Type#NULL NULL} Value.
     *
     * @param path path of the field that needs to be updated
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setNull(FieldPath path) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(null);
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified value.
     *
     * @param path  path of the field that needs to be updated
     * @param value The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, Value value) {
        return set(FieldPath.parseFrom(path), value);
    }

    /**
     * Sets the field at the given FieldPath to the specified value.
     *
     * @param path  path of the field that needs to be updated
     * @param value The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, Value value) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(value);
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code boolean} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, boolean b) {
        return set(FieldPath.parseFrom(path), b);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code boolean} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, boolean b) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(b));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code byte} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, byte b) {
        return set(FieldPath.parseFrom(path), b);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code byte} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, byte b) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(b));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code short} value.
     *
     * @param path path of the field that needs to be updated
     * @param s    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, short s) {
        return set(FieldPath.parseFrom(path), s);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code short} value.
     *
     * @param path path of the field that needs to be updated
     * @param s    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, short s) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(s));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code int} value.
     *
     * @param path path of the field that needs to be updated
     * @param i    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, int i) {
        return set(FieldPath.parseFrom(path), i);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code int} value.
     *
     * @param path path of the field that needs to be updated
     * @param i    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, int i) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(i));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code long} value.
     *
     * @param path path of the field that needs to be updated
     * @param l    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, long l) {
        return set(FieldPath.parseFrom(path), l);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code long} value.
     *
     * @param path path of the field that needs to be updated
     * @param l    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, long l) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(l));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code float} value.
     *
     * @param path path of the field that needs to be updated
     * @param f    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, float f) {
        return set(FieldPath.parseFrom(path), f);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code float} value.
     *
     * @param path path of the field that needs to be updated
     * @param f    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, float f) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(f));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code double} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, double d) {
        return set(FieldPath.parseFrom(path), d);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code double} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, double d) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(d));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code String} value.
     *
     * @param path  path of the field that needs to be updated
     * @param value The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, String value) {
        return set(FieldPath.parseFrom(path), value);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code String} value.
     *
     * @param path  path of the field that needs to be updated
     * @param value The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, String value) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(value));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code BigDecimal} value.
     *
     * @param path path of the field that needs to be updated
     * @param bd   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, BigDecimal bd) {
        return set(FieldPath.parseFrom(path), bd);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code BigDecimal} value.
     *
     * @param path path of the field that needs to be updated
     * @param bd   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, BigDecimal bd) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(bd));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Date} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, ODate d) {
        return set(FieldPath.parseFrom(path), d);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Date} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, ODate d) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(d));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Time} value.
     *
     * @param path path of the field that needs to be updated
     * @param t    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, OTime t) {
        return set(FieldPath.parseFrom(path), t);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Time} value.
     *
     * @param path path of the field that needs to be updated
     * @param t    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, OTime t) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(t));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Timestamp} value.
     *
     * @param path path of the field that needs to be updated
     * @param ts   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, OTimestamp ts) {
        return set(FieldPath.parseFrom(path), ts);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Timestamp} value.
     *
     * @param path path of the field that needs to be updated
     * @param ts   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, OTimestamp ts) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(ts));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Interval} value.
     *
     * @param path path of the field that needs to be updated
     * @param intv The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, OInterval intv) {
        return set(FieldPath.parseFrom(path), intv);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Interval} value.
     *
     * @param path path of the field that needs to be updated
     * @param intv The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, OInterval intv) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(intv));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code ByteBuffer}.
     *
     * @param path path of the field that needs to be updated
     * @param bb   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, ByteBuffer bb) {
        return set(FieldPath.parseFrom(path), bb);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code ByteBuffer}.
     *
     * @param path path of the field that needs to be updated
     * @param bb   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, ByteBuffer bb) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(new HValue(bb));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code List}.
     *
     * @param path path of the field that needs to be updated
     * @param list The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, List<? extends Object> list) {
        return set(FieldPath.parseFrom(path), list);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code List}.
     *
     * @param path path of the field that needs to be updated
     * @param list The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, List<? extends Object> list) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromList(list));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Map}.
     *
     * @param path path of the field that needs to be updated
     * @param map  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, Map<String, ? extends Object> map) {
        return set(FieldPath.parseFrom(path), map);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Map}.
     *
     * @param path path of the field that needs to be updated
     * @param map  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, Map<String, ? extends Object> map) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromMap(map));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Document}.
     *
     * @param path path of the field that needs to be updated
     * @param doc  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(String path, Document doc) {
        return set(FieldPath.parseFrom(path), doc);
    }

    /**
     * Sets the field at the given FieldPath to the specified {@code Document}.
     *
     * @param path path of the field that needs to be updated
     * @param doc  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation set(FieldPath path, Document doc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromDocument(doc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to {@link Type#NULL NULL} Value.
     *
     * @param path FieldPath in the document that needs to be updated
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplaceNull(String path) {
        return setOrReplaceNull(FieldPath.parseFrom(path));
    }

    /**
     * Sets or replaces the field at the given FieldPath to {@link Type#NULL NULL} Value.
     *
     * @param path FieldPath in the document that needs to be updated
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplaceNull(FieldPath path) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(null);
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the new value.
     *
     * @param path  FieldPath in the document that needs to be updated
     * @param value The new value to set or replace at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, Value value) {
        return setOrReplace(FieldPath.parseFrom(path), value);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the new value.
     *
     * @param path  FieldPath in the document that needs to be updated
     * @param value The new value to set or replace at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, Value value) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(value);
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code boolean} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, boolean b) {
        return setOrReplace(FieldPath.parseFrom(path), b);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code boolean} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, boolean b) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(b));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code byte} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, byte b) {
        return setOrReplace(FieldPath.parseFrom(path), b);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code byte} value.
     *
     * @param path path of the field that needs to be updated
     * @param b    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, byte b) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(b));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code short} value.
     *
     * @param path path of the field that needs to be updated
     * @param s    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, short s) {
        return setOrReplace(FieldPath.parseFrom(path), s);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code short} value.
     *
     * @param path path of the field that needs to be updated
     * @param s    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, short s) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(s));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code int} value.
     *
     * @param path path of the field that needs to be updated
     * @param i    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, int i) {
        return setOrReplace(FieldPath.parseFrom(path), i);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code int} value.
     *
     * @param path path of the field that needs to be updated
     * @param i    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, int i) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(i));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code long} value.
     *
     * @param path path of the field that needs to be updated
     * @param l    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, long l) {
        return setOrReplace(FieldPath.parseFrom(path), l);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code long} value.
     *
     * @param path path of the field that needs to be updated
     * @param l    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, long l) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(l));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code float} value.
     *
     * @param path path of the field that needs to be updated
     * @param f    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, float f) {
        return setOrReplace(FieldPath.parseFrom(path), f);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code float} value.
     *
     * @param path path of the field that needs to be updated
     * @param f    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, float f) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(f));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code double} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, double d) {
        return setOrReplace(FieldPath.parseFrom(path), d);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code double} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, double d) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(d));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code String} value.
     *
     * @param path   path of the field that needs to be updated
     * @param string The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, String string) {
        return setOrReplace(FieldPath.parseFrom(path), string);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code String} value.
     *
     * @param path   path of the field that needs to be updated
     * @param string The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, String string) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(string));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code BigDecimal} value.
     *
     * @param path path of the field that needs to be updated
     * @param bd   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, BigDecimal bd) {
        return setOrReplace(FieldPath.parseFrom(path), bd);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code BigDecimal} value.
     *
     * @param path path of the field that needs to be updated
     * @param bd   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, BigDecimal bd) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(bd));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Date} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, ODate d) {
        return setOrReplace(FieldPath.parseFrom(path), d);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Date} value.
     *
     * @param path path of the field that needs to be updated
     * @param d    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, ODate d) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(d));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Time} value.
     *
     * @param path path of the field that needs to be updated
     * @param t    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, OTime t) {
        return setOrReplace(FieldPath.parseFrom(path), t);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Time} value.
     *
     * @param path path of the field that needs to be updated
     * @param t    The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, OTime t) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(t));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Timestamp} value.
     *
     * @param path path of the field that needs to be updated
     * @param ts   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, OTimestamp ts) {
        return setOrReplace(FieldPath.parseFrom(path), ts);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Timestamp} value.
     *
     * @param path path of the field that needs to be updated
     * @param ts   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, OTimestamp ts) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(ts));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Interval}.
     *
     * @param path path of the field that needs to be updated
     * @param intv The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, OInterval intv) {
        return setOrReplace(FieldPath.parseFrom(path), intv);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Interval}.
     *
     * @param path path of the field that needs to be updated
     * @param intv The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, OInterval intv) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(intv));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code ByteBuffer}.
     *
     * @param path path of the field that needs to be updated
     * @param bb   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, ByteBuffer bb) {
        return setOrReplace(FieldPath.parseFrom(path), bb);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code ByteBuffer}.
     *
     * @param path path of the field that needs to be updated
     * @param bb   The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, ByteBuffer bb) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(new HValue(bb));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code List}.
     *
     * @param path path of the field that needs to be updated
     * @param list The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, List<? extends Object> list) {
        return setOrReplace(FieldPath.parseFrom(path), list);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code List}.
     *
     * @param path path of the field that needs to be updated
     * @param list The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, List<? extends Object> list) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromList(list));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Map}.
     *
     * @param path path of the field that needs to be updated
     * @param map  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, Map<String, ? extends Object> map) {
        return setOrReplace(FieldPath.parseFrom(path), map);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Map}.
     *
     * @param path path of the field that needs to be updated
     * @param map  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, Map<String, ? extends Object> map) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromMap(map));
        mutationOps.add(op);

        return this;
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Document}.
     *
     * @param path path of the field that needs to be updated
     * @param doc  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(String path, Document doc) {
        return setOrReplace(FieldPath.parseFrom(path), doc);
    }

    /**
     * Sets or replaces the field at the given FieldPath to the specified
     * {@code Document}.
     *
     * @param path path of the field that needs to be updated
     * @param doc  The new value to set at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation setOrReplace(FieldPath path, Document doc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.SET_OR_REPLACE);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromDocument(doc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Appends elements of the given list to an existing ARRAY at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the specified List. If the field already exists, but is not of ARRAY type,
     * then this operation will fail.
     *
     * @param path path of the field that needs to be appended
     * @param list The List to append at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(String path, List<? extends Object> list) {
        return append(FieldPath.parseFrom(path), list);
    }

    /**
     * Appends elements of the given list to an existing ARRAY at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the specified List. If the field already exists, but is not of ARRAY type,
     * then this operation will fail.
     *
     * @param path path of the field that needs to be appended
     * @param list The List to append at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(FieldPath path, List<? extends Object> list) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.APPEND);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromList(list));
        mutationOps.add(op);

        return this;
    }

    /**
     * Appends the given string to an existing STRING at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the specified String. If the field already exists, but is not of STRING type,
     * then this operation will fail.
     *
     * @param path   path of the field that needs to be appended
     * @param string The String to append at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(String path, String string) {
        return append(FieldPath.parseFrom(path), string);
    }

    /**
     * Appends the given string to an existing STRING at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the specified String. If the field already exists, but is not of STRING type,
     * then this operation will fail.
     *
     * @param path   path of the field that needs to be appended
     * @param string The String to append at the FieldPath
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(FieldPath path, String string) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.APPEND);
        op.setFieldPath(path);
        op.setOpValue(new HValue(string));
        mutationOps.add(op);

        return this;
    }

    /**
     * Appends the given byte array to an existing BINARY value at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the BINARY value specified by the given byte array. If the field already exists,
     * but is not of BINARY type, then this operation will fail.
     *
     * @param path   the FieldPath to apply this append operation
     * @param value  the byte array to append
     * @param offset offset in byte array
     * @param len    length in byte array
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(String path, byte[] value, int offset, int len) {
        return append(FieldPath.parseFrom(path), value, offset, len);
    }

    /**
     * Appends the given byte array to an existing BINARY value at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the BINARY value specified by the given byte array. If the field already exists,
     * but is not of BINARY type, then this operation will fail.
     *
     * @param path   the FieldPath to apply this append operation
     * @param value  the byte array to append
     * @param offset offset in byte array
     * @param len    length in byte array
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(FieldPath path, byte[] value, int offset, int len) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.APPEND);
        op.setFieldPath(path);
        op.setOpValue(new HValue(ByteBuffer.wrap(value, offset, len)));
        mutationOps.add(op);

        return this;
    }

    /**
     * Appends the given byte array to an existing BINARY value at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the BINARY value specified by the given byte array. If the field already exists,
     * but is not of BINARY type, then this operation will fail.
     *
     * @param path  the FieldPath to apply this append operation
     * @param value the byte array to append
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(String path, byte[] value) {
        return append(FieldPath.parseFrom(path), value);
    }

    /**
     * Appends the given byte array to an existing BINARY value at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the BINARY value specified by the given byte array. If the field already exists,
     * but is not of BINARY type, then this operation will fail.
     *
     * @param path  the FieldPath to apply this append operation
     * @param value the byte array to append
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(FieldPath path, byte[] value) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.APPEND);
        op.setFieldPath(path);
        op.setOpValue(new HValue(ByteBuffer.wrap(value)));
        mutationOps.add(op);

        return this;
    }

    /**
     * Appends the given ByteBuffer to an existing BINARY value at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the BINARY value specified by the given ByteBuffer. If the field already exists,
     * but is not of BINARY type, then this operation will fail.
     *
     * @param path  the FieldPath to apply this append operation
     * @param value the ByteBuffer to append
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(String path, ByteBuffer value) {
        return append(FieldPath.parseFrom(path), value);
    }

    /**
     * Appends the given ByteBuffer to an existing BINARY value at the given FieldPath.
     * <br/><br/>
     * If the field doesn't exist on server, it will be created and will be set to
     * the BINARY value specified by the given ByteBuffer. If the field already exists,
     * but is not of BINARY type, then this operation will fail.
     *
     * @param path  the FieldPath to apply this append operation
     * @param value the ByteBuffer to append
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation append(FieldPath path, ByteBuffer value) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.APPEND);
        op.setFieldPath(path);
        op.setOpValue(new HValue(value));
        mutationOps.add(op);

        return this;
    }

    /**
     * Merges the existing MAP at the given FieldPath with the specified Document.
     * <br/><br/>
     *
     * @param path the FieldPath to apply this merge operation
     * @param doc  the document to be merged
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation merge(String path, Document doc) {
        return merge(FieldPath.parseFrom(path), doc);
    }

    /**
     * Merges the existing MAP at the given FieldPath with the specified Document.
     * <br/><br/>
     *
     * @param path the FieldPath to apply this merge operation
     * @param doc  the document to be merged
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation merge(FieldPath path, Document doc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.MERGE);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromDocument(doc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Merges the existing MAP at the given FieldPath with the specified Map.
     * <br/><br/>
     *
     * @param path the FieldPath to apply this merge operation
     * @param map  the Map to be merged
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation merge(String path, Map<String, Object> map) {
        return merge(FieldPath.parseFrom(path), map);
    }

    /**
     * Merges the existing MAP at the given FieldPath with the specified Map.
     * <br/><br/>
     *
     * @param path the FieldPath to apply this merge operation
     * @param map  the Map to be merged
     * @return {@code this} for chained invocation
     */
    public HDocumentMutation merge(FieldPath path, Map<String, Object> map) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.MERGE);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromMap(map));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, byte inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, byte inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, short inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, short inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, int inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, int inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, long inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, long inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, float inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the field specified by the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, float inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    public HDocumentMutation increment(String path, double inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, double inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, BigDecimal inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, BigDecimal inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(new HValue(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(String path, Number inc) {
        return increment(FieldPath.parseFrom(path), inc);
    }

    /**
     * Atomically increment the existing value at given the FieldPath by the given value.
     *
     * @param path the FieldPath to apply this increment operation
     * @param inc  increment to apply to a field - can be positive or negative
     */
    public HDocumentMutation increment(FieldPath path, Number inc) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.INCREMENT);
        op.setFieldPath(path);
        op.setOpValue(HValue.initFromObject(inc));
        mutationOps.add(op);

        return this;
    }

    /**
     * Deletes the field at the given path.
     * <br/><br/>
     * If the field does not exist, the mutation operation will silently succeed.
     * For example, if a delete operation is attempted on {@code "a.b.c"}, and the
     * field {@code "a.b"} is an array, then {@code "a.b.c"} will not be deleted.
     *
     * @param path the FieldPath to delete
     */
    public HDocumentMutation delete(String path) {
        return delete(FieldPath.parseFrom(path));
    }

    /**
     * Deletes the field at the given path.
     * <br/><br/>
     * If the field does not exist, the mutation operation will silently succeed.
     * For example, if a delete operation is attempted on {@code "a.b.c"}, and the
     * field {@code "a.b"} is an array, then {@code "a.b.c"} will not be deleted.
     *
     * @param path the FieldPath to delete
     */
    public HDocumentMutation delete(FieldPath path) {
        MutationOp op = new MutationOp();
        op.setType(MutationOp.Type.DELETE);
        op.setFieldPath(path);
        mutationOps.add(op);

        return this;
    }


    public Iterator<MutationOp> iterator() {
        return mutationOps.iterator();
    }

    public void fillMutations(RowMutations mutations, String family, HDocument document) {
        try {
            for (MutationOp mutationOp : mutationOps) {
                MutationOp.Type type = mutationOp.getType();
                switch (type) {
                    case SET:
                        fillSet(mutations, family, document, mutationOp);
                    case SET_OR_REPLACE:
                        fillSetOrReplace(mutations, family, mutationOp);
                        break;
                    case DELETE:
                        fillDelete(mutations, family, document, mutationOp);
                        break;
                    case INCREMENT:
                        fillIncrement(mutations, family, document, mutationOp);
                        break;
                    case APPEND:
                        fillAppend(mutations, family, document, mutationOp);
                        break;
                    case MERGE:
                        fillMerge(mutations, family, document, mutationOp);
                        break;
                }
            }
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    private void fillSet(RowMutations mutations, String family, HDocument document, MutationOp mutationOp) throws java.io.IOException {
        FieldPath path = mutationOp.getFieldPath();
        HValue newValue = HValue.initFromValue(mutationOp.getOpValue());
        if (document != null) document.checkHValue(path, newValue);
        HDocument doc = new HDocument();
        doc.setHValue(path, newValue);
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

    private void fillSetOrReplace(RowMutations mutations, String family, MutationOp mutationOp) throws java.io.IOException {
        FieldPath path = mutationOp.getFieldPath();
        HDocument doc = new HDocument();
        doc.setHValue(path, HValue.initFromValue(mutationOp.getOpValue()));
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

    private void fillDelete(RowMutations mutations, String family, HDocument document, MutationOp mutationOp) throws IOException {
        FieldPath path = mutationOp.getFieldPath();
        Delete delete = new Delete(mutations.getRow());
        if (document != null) {
            HValue value = document.getHValue(path);
            if (value != null) value.fillDelete(delete, family, path);
        } else {
            delete.addColumns(Bytes.toBytes(family), Bytes.toBytes(path.asPathString()));
        }
        if (!delete.isEmpty()) {
            mutations.add(delete);
        }
    }

    private void fillIncrement(RowMutations mutations, String family, HDocument document, MutationOp mutationOp) throws IOException {
        if (mutationOp.getOpValue() == null) throw new IllegalArgumentException("Null passed to " + mutationOp.getType());
        FieldPath path = mutationOp.getFieldPath();
        HValue incrValue = HValue.initFromValue(mutationOp.getOpValue());
        Object incrObj = incrValue.getObject();
        if (!(incrObj instanceof Number)) {
            throw new IllegalArgumentException("Cannot increment with non-number");
        }
        Number incrNum = (Number)incrObj;

        HValue oldValue = document != null ? document.checkHValue(path, incrValue) : null;
        HValue newValue;
        if (oldValue != null) {
            Object oldObj = oldValue.getObject();
            if (!(oldObj instanceof Number)) {
                throw new IllegalArgumentException("Cannot increment non-number");
            }
            Number oldNum = (Number) oldObj;

            switch (oldValue.getType()) {
                case BYTE:
                    newValue = new HValue((byte) (oldNum.byteValue() + incrNum.byteValue()));
                    break;
                case SHORT:
                    newValue = new HValue((short) (oldNum.shortValue() + incrNum.shortValue()));
                    break;
                case INT:
                    newValue = new HValue((int) (oldNum.intValue() + incrNum.intValue()));
                    break;
                case LONG:
                    newValue = new HValue((long) (oldNum.longValue() + incrNum.longValue()));
                    break;
                case FLOAT:
                    newValue = new HValue((float) (oldNum.floatValue() + incrNum.floatValue()));
                    break;
                case DOUBLE:
                    newValue = new HValue((double) (oldNum.doubleValue() + incrNum.doubleValue()));
                    break;
                case DECIMAL:
                    newValue = new HValue(new BigDecimal(oldNum.toString()).add(new BigDecimal(incrNum.toString())));
                    break;
                default:
                    throw new IllegalArgumentException("Cannot increment non-number");
            }
        } else {
            newValue = incrValue;
        }
        HDocument doc = new HDocument();
        doc.setHValue(path, newValue);
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

    private void fillAppend(RowMutations mutations, String family, HDocument document, MutationOp mutationOp) throws IOException {
        if (mutationOp.getOpValue() == null) throw new IllegalArgumentException("Null passed to " + mutationOp.getType());
        FieldPath path = mutationOp.getFieldPath();
        HValue appendValue = HValue.initFromValue(mutationOp.getOpValue());
        HValue oldValue = document != null ? document.checkHValue(path, appendValue) : null;
        switch (appendValue.getType()) {
            case ARRAY:
                fillAppendArray(mutations, family, mutationOp, oldValue, appendValue);
                break;
            case BINARY:
                fillAppendBinary(mutations, family, mutationOp, oldValue, appendValue);
                break;
            case STRING:
                fillAppendString(mutations, family, mutationOp, oldValue, appendValue);
                break;
            default:
                throw new IllegalArgumentException("Cannot append with " + appendValue.getType());
        }
    }

    private void fillAppendArray(RowMutations mutations, String family, MutationOp mutationOp, Value oldValues, Value newValues) throws IOException {
        FieldPath path = mutationOp.getFieldPath();
        HList oldList = oldValues != null ? (HList) oldValues.getList() : new HList();
        HList newList = newValues != null ? (HList) newValues.getList() : new HList();
        HDocument doc = new HDocument();
        for (int i = 0; i < newList.size(); i++) {
            HValue newValue = newList.getHValue(i);
            doc.setHValue(FieldPath.parseFrom(path.asPathString() + "[" + (oldList.size() + i) + "]"), newValue);
        }
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

    private void fillAppendBinary(RowMutations mutations, String family, MutationOp mutationOp, Value oldValue, Value appendValue) throws IOException {
        FieldPath path = mutationOp.getFieldPath();
        ByteBuffer oldBytes = oldValue != null ? oldValue.getBinary() : ByteBuffer.allocate(0);
        ByteBuffer appendBytes = appendValue.getBinary();
        ByteBuffer newBytes = ByteBuffer.allocate(oldBytes.limit() + appendBytes.limit());
        newBytes.put(oldBytes).put(appendBytes);
        HValue newValue = new HValue(newBytes);
        HDocument doc = new HDocument();
        doc.setHValue(path, newValue);
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

    private void fillAppendString(RowMutations mutations, String family, MutationOp mutationOp, Value oldValue, Value appendValue) throws IOException {
        FieldPath path = mutationOp.getFieldPath();
        String oldString = oldValue != null ? oldValue.getString() : "";
        String appendString = appendValue.getString();
        HValue newValue = new HValue(oldString + appendString);
        HDocument doc = new HDocument();
        doc.setHValue(path, newValue);
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

    private void fillMerge(RowMutations mutations, String family, HDocument document, MutationOp mutationOp) throws IOException {
        if (mutationOp.getOpValue() == null) throw new IllegalArgumentException("Null passed to " + mutationOp.getType());
        FieldPath path = mutationOp.getFieldPath();
        HValue newValues = HValue.initFromValue(mutationOp.getOpValue());
        if (document != null) document.checkHValue(path, newValues);
        HDocument doc = new HDocument();
        for (Map.Entry<String, Value> entry : (HDocument) newValues.getMap()) {
            doc.setHValue(FieldPath.parseFrom(path.asPathString() + "." + entry.getKey()), HValue.initFromValue(entry.getValue()));
        }
        Put put = new Put(mutations.getRow());
        doc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
    }

}
