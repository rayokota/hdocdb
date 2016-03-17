package io.hdocdb.store;

import io.hdocdb.HDocument;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.Value.Type;

public class IndexFieldPath {

    public static final String ORDER_PATH = "order";
    public static final String PATH_PATH = "path";
    public static final String TYPE_PATH = "type";

    private FieldPath path;
    private Value.Type type;
    private Order order;

    public IndexFieldPath(FieldPath path, Value.Type type, Order order) {
        this.path = path;
        this.type = type;
        this.order = order;
    }

    public IndexFieldPath(Document document) {
        this.path = FieldPath.parseFrom(document.getString(PATH_PATH));
        this.type = Value.Type.valueOf(document.getString(TYPE_PATH));
        this.order = Order.valueOf(document.getString(ORDER_PATH));
    }

    public Document asDocument() {
        HDocument doc = new HDocument();
        doc.set(PATH_PATH, path.asPathString());
        doc.set(TYPE_PATH, type.toString());
        doc.set(ORDER_PATH, order.toString());
        return doc;
    }

    public FieldPath getPath() {
        return path;
    }

    public Type getType() {
        return type;
    }

    public Order getOrder() {
        return order;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexFieldPath that = (IndexFieldPath) o;

        if (!path.equals(that.path)) return false;
        return type == that.type;

    }

    @Override
    public int hashCode() {
        int result = path.hashCode();
        result = 31 * result + type.hashCode();
        return result;
    }
}
