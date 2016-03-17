package io.hdocdb;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.ojai.FieldPath;
import org.ojai.FieldSegment;
import org.ojai.store.exceptions.StoreException;

import java.util.Iterator;

public abstract class HContainer extends HValue {

    public abstract int size();

    public abstract boolean isEmpty();

    public void clear() {
        clearHValues();
    }

    public HValue checkHValue(FieldPath path, HValue value) throws StoreException {
        return checkHValue(path.iterator(), value);
    }

    public abstract HValue checkHValue(Iterator<FieldSegment> path, HValue value) throws StoreException;

    public HValue getHValue(FieldPath path) {
        return getHValue(path.iterator());
    }

    public abstract HValue getHValue(Iterator<FieldSegment> path);

    public void setHValue(FieldPath path, HValue value) {
        setHValue(path.iterator(), value);
    }

    public abstract void setHValue(Iterator<FieldSegment> path, HValue value);

    public void removeHValue(FieldPath path) {
        removeHValue(path.iterator());
    }

    public abstract void removeHValue(Iterator<FieldSegment> path);

    public abstract void clearHValues();

    protected abstract boolean compareValueTimestamps();

    protected abstract void setCompareValueTimestamps(boolean compare);

    protected abstract HContainer reindexArrays();

    public abstract void fillDelete(Delete delete, String family, FieldPath parentPath);

    public abstract void fillPut(Put put, String family, FieldPath parentPath);
}
