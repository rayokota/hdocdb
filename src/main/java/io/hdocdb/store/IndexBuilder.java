package io.hdocdb.store;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.exceptions.StoreException;

import java.util.AbstractList;
import java.util.List;

public class IndexBuilder extends AbstractList<IndexFieldPath> {

    private final HDocumentDB db;
    private final HDocumentCollection collection;
    private final Table table;
    private final String name;
    private final List<IndexFieldPath> elements = Lists.newArrayList();
    private boolean async;

    public IndexBuilder(HDocumentDB db, HDocumentCollection collection, Table table, String name) {
        this.db = db;
        this.collection = collection;
        this.table = table;
        this.name = name;
        this.async = true;
    }

    public Index build() {
        Index index = new Index(name, elements);
        // we create the index first so that newly created rows will be subsequently indexed
        db.createIndex(table.getName(), index);
        collection.populateIndex(name, async);
        return index;
    }

    public String getName() {
        return name;
    }

    public int size() {
        return elements.size();
    }

    public IndexFieldPath get(int index) {
        return elements.get(index);
    }

    public void add(int index, IndexFieldPath path) {
        elements.add(index, path);
    }

    public IndexBuilder add(String path, Value.Type type) {
        return add(FieldPath.parseFrom(path), type, Order.ASCENDING);
    }

    public IndexBuilder add(String path, Value.Type type, Order order) {
        return add(FieldPath.parseFrom(path), type, order);
    }

    public IndexBuilder add(FieldPath path, Value.Type type, Order order) {
        if (type == Value.Type.ARRAY || type == Value.Type.MAP) {
            throw new StoreException("Cannot support indexes on containers");
        }
        add(new IndexFieldPath(path, type, order));
        return this;
    }

    public IndexBuilder setAsync(boolean async) {
        this.async = async;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        IndexBuilder that = (IndexBuilder) o;

        return name.equals(that.name) && super.equals(o);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }
}
