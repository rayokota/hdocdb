package io.hdocdb.execute;

import io.hdocdb.HDocumentStream;
import io.hdocdb.store.IndexQueries;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.DocumentStream;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;

import java.io.IOException;

public class QueryPlan {

    private Table table;
    private Get get;
    private IndexQueries indexQueries;
    private Scan scan;
    private boolean reindexArrays;
    private QueryCondition condition;
    private String[] paths;

    public QueryPlan(Table table, Get get, boolean reindexArrays, QueryCondition c, String... paths) {
        this.table = table;
        this.get = get;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public QueryPlan(Table table, IndexQueries indexQueries,
                     boolean reindexArrays, QueryCondition c, String... paths) {
        this.table = table;
        this.indexQueries = indexQueries;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public QueryPlan(Table table, Scan scan, boolean reindexArrays, QueryCondition c, String... paths) {
        this.table = table;
        this.scan = scan;
        this.reindexArrays = reindexArrays;
        this.condition = c;
        this.paths = paths;
    }

    public DocumentStream execute() throws StoreException {
        try {
            if (get != null) {
                return new HDocumentStream(new Result[]{table.get(get)}, reindexArrays, condition, paths);
            } else if (scan != null) {
                return new HDocumentStream(table.getScanner(scan), reindexArrays, condition, paths);
            } else if (indexQueries != null) {
                return new HDocumentStream(table, indexQueries, reindexArrays, condition, paths);
            } else {
                throw new IllegalStateException();
            }
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

}
