package io.hdocdb.store;

import org.apache.hadoop.hbase.client.Get;

import java.nio.ByteBuffer;

public class IndexQuery {

    private final ByteBuffer indexRowKey;
    private final long indexTs;
    private final Get get;

    public IndexQuery(ByteBuffer indexRowKey, long indexTs, Get get) {
        this.indexRowKey = indexRowKey;
        this.get = get;
        this.indexTs = indexTs;
    }

    public ByteBuffer getIndexRowKey() {
        return indexRowKey;
    }

    public long getIndexTs() {
        return indexTs;
    }

    public Get getQuery() {
        return get;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexQuery that = (IndexQuery) o;

        if (indexTs != that.indexTs) return false;
        if (!indexRowKey.equals(that.indexRowKey)) return false;
        return get.equals(that.get);

    }

    @Override
    public int hashCode() {
        int result = indexRowKey.hashCode();
        result = 31 * result + (int) (indexTs ^ (indexTs >>> 32));
        result = 31 * result + get.hashCode();
        return result;
    }
}
