package io.hdocdb.store;

import com.google.common.base.Ticker;
import com.google.common.testing.FakeTicker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.ojai.Value;

import java.io.IOException;

public class HDocumentDBTest {
    protected static final TableName TABLE_MAIN = TableName.valueOf("testmain");
    protected static final TableName IDX_TABLE_MAIN = TableName.valueOf("_IDX_testmain");
    protected static final TableName TABLE_TEMP = TableName.valueOf("testtemp");
    protected static final TableName IDX_TABLE_TEMP = TableName.valueOf("_IDX_testtemp");

    protected static Ticker ticker;
    protected static HDocumentDB hdocdb;
    protected static HDocumentCollection mainColl;

    protected static boolean useMock = true;

    public static void setup() throws IOException {
        ticker = new FakeTicker();
        if (!useMock) {
            Configuration config = new Configuration();
            config.set("hbase.zookeeper.quorum", "127.0.0.1");
            config.set("zookeeper.znode.parent", "/hbase-unsecure");
            hdocdb = new HDocumentDB(config, ticker);
        } else {
            hdocdb = new InMemoryHDocumentDB(ticker);
        }
        mainColl = getDocumentCollection(TABLE_MAIN);
        mainColl.createIndex("testindex", "last_name", Value.Type.STRING, Order.ASCENDING, false);
    }

    protected static HDocumentCollection getTempDocumentCollection() {
        return getDocumentCollection(TABLE_TEMP);
    }

    protected static HDocumentCollection getDocumentCollection(TableName name) {
        hdocdb.dropCollection(name);
        return hdocdb.getCollection(name);
    }

    protected static void closeDocumentCollection(HDocumentCollection coll) {
        coll.close();
        hdocdb.dropCollection(coll.getTableName());
    }

    public static void teardown() throws IOException {
        closeDocumentCollection(mainColl);
    }
}
