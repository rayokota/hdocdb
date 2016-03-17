package io.hdocdb.store;

import com.google.common.base.Ticker;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mock.MockHTable;

import java.io.IOException;
import java.util.Map;

public class MockHDocumentDB extends HDocumentDB {

    private Map<TableName, Table> tables;
    private Ticker ticker;

    public MockHDocumentDB() throws IOException {
        super(null);
    }

    public MockHDocumentDB(Ticker ticker) throws IOException {
        super(null, ticker);
    }

    private Map<TableName, Table> getTables() {
        if (tables == null) tables = Maps.newHashMap();
        return tables;
    }

    protected void createTable(TableName name) throws IOException {
        if (!tableExists(name)) {
            getTables().put(name, new MockHTable(name, DEFAULT_FAMILY));
        }
    }

    protected void createTable(TableName name, int maxVersions, boolean keepDeleted) throws IOException {
        if (!tableExists(name)) {
            getTables().put(name, new MockHTable(name, DEFAULT_FAMILY));
        }
    }

    protected boolean tableExists(TableName name) throws IOException {
        return getTables().containsKey(name);
    }

    protected Table getTable(TableName name) throws IOException {
        return getTables().get(name);
    }

    protected void dropTable(TableName name) throws IOException {
        Table table = getTables().get(name);
        if (table != null) ((MockHTable)table).clear();
    }
}
