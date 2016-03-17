package io.hdocdb.store;

import com.google.common.base.Ticker;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import io.hdocdb.HDocument;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HDocumentDB extends AbstractMap<String, HDocumentCollection> {

    protected static final String CATALOG_TABLE = "_CATALOG_";
    protected static final String DEFAULT_FAMILY = "c";
    protected static final boolean DEFAULT_INDEX_KEEP_DELETED = true;
    protected static final int DEFAULT_INDEX_MAX_VERSIONS = 5;
    protected static final String INDEX_PREFIX = "_IDX_";
    protected static final String INDEXES_PATH = "indexes";

    /* How often to refresh the index cache */
    protected static final int INDEX_CACHE_REFRESH_SECS = 1;
    /* How long to wait before an index state change is propagated to other index caches */
    protected static final int INDEX_STATE_CHANGE_DELAY_SECS = 2;

    private Connection connection;
    private HDocumentCollection indexCollection;
    private LoadingCache<TableName, Map<String, Index>> indexes;

    public HDocumentDB(Connection connection) throws IOException {
        this(connection, Ticker.systemTicker());
    }

    public HDocumentDB(Connection connection, Ticker ticker) throws IOException {
        this.connection = connection;
        this.indexCollection = getCollection(TableName.valueOf(CATALOG_TABLE), null);
        this.indexes = CacheBuilder.newBuilder()
                .refreshAfterWrite(INDEX_CACHE_REFRESH_SECS, TimeUnit.SECONDS)
                .ticker(ticker)
                .build(new CacheLoader<TableName, Map<String, Index>>() {
                    @Override
                    public Map<String, Index> load(TableName key) throws Exception {
                        return convertIndexDocument(indexCollection.findById(key.toString()));
                    }
                });
    }

    public HDocumentCollection createCollection(String name) {
        return createCollection(TableName.valueOf(name));
    }

    public HDocumentCollection createCollection(TableName name) {
        return createCollection(name, getIndexTableName(name));
    }

    public HDocumentCollection createCollection(TableName name, TableName indexTableName) {
        try {
            createTable(name);
            if (indexTableName != null) {
                // allow multiple versions of the index table to allow scans to complete when the index is dropped
                createTable(indexTableName, DEFAULT_INDEX_MAX_VERSIONS, DEFAULT_INDEX_KEEP_DELETED);
            }
            return new HDocumentCollection(this,
                    getTable(name),
                    indexTableName != null ? getTable(indexTableName) : null,
                    DEFAULT_FAMILY);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    private TableName getIndexTableName(TableName name) {
        return TableName.valueOf(name.getNamespaceAsString(), INDEX_PREFIX + name.getNameAsString());
    }

    public boolean collectionExists(String name) {
        return collectionExists(TableName.valueOf(name));
    }

    public boolean collectionExists(TableName name) {
        try {
            return tableExists(name);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    public HDocumentCollection getCollection(String name) {
        return getCollection(TableName.valueOf(name));
    }

    public HDocumentCollection getCollection(TableName name) {
        return getCollection(name, getIndexTableName(name));
    }

    public HDocumentCollection getCollection(TableName name, TableName indexTableName) {
        try {
            if (!collectionExists(name)) {
                return createCollection(name, indexTableName);
            }
            return new HDocumentCollection(this,
                    getTable(name),
                    indexTableName != null ? getTable(indexTableName) : null,
                    DEFAULT_FAMILY);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    public void dropCollection(String name) {
        dropCollection(TableName.valueOf(name));
    }

    public void dropCollection(TableName name) {
        try {
            dropTable(name);
            dropTable(getIndexTableName(name));
            dropIndexes(name);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    public List<HDocumentCollection> listCollections() {
        throw new UnsupportedOperationException();
    }

    protected void createIndex(TableName tableName, Index index) {
        try {
            indexes.refresh(tableName);
            // this may overwrite a dropped index with the same name
            Index oldIndex = indexes.get(tableName).get(index.getName());
            if (oldIndex != null && oldIndex.getState() != Index.State.DROPPED) {
                throw new StoreException("Index with name " + index.getName() + " already exists");
            }

            boolean success;
            HDocumentMutation mutation = new HDocumentMutation().setOrReplace(
                    INDEXES_PATH + "." + index.getName(), index.asDocument());
            if (oldIndex == null) {
                success = indexCollection.checkAndMutate(tableName.toString(),
                        new HQueryCondition().notExists(INDEXES_PATH + "." + index.getName()),
                        mutation);
            } else {
                success = indexCollection.checkAndMutate(tableName.toString(),
                        new HQueryCondition().is(INDEXES_PATH + "." + index.getName() + "." + Index.STATE_PATH,
                                QueryCondition.Op.EQUAL, Index.State.DROPPED.toString()),
                        mutation);
            }
            if (!success) {
                throw new StoreException("Could not create index " + index.getName());
            }

            // update local cache
            indexes.get(tableName).put(index.getName(), index);
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    protected void updateIndexState(TableName tableName, String indexName, Index.State newState) {
        try {
            indexes.refresh(tableName);
            Index oldIndex = indexes.get(tableName).get(indexName);
            if (oldIndex == null) {
                throw new StoreException("Index with name " + indexName + " does not exist");
            }
            Index.State oldState = oldIndex.getState();

            boolean doTransition = true;
            switch (oldState) {
                case CREATED:
                    break;
                case BUILDING:
                    if (newState == Index.State.CREATED) {
                        doTransition = false;
                    }
                    break;
                case ACTIVE:
                    if (newState == Index.State.CREATED) {
                        doTransition = false;
                    }
                    break;
                case INACTIVE:
                    if (newState == Index.State.CREATED || newState == Index.State.BUILDING || newState == Index.State.ACTIVE) {
                        doTransition = false;
                    }
                    break;
                case DROPPED:
                    if (newState != Index.State.CREATED) {
                        doTransition = false;
                    }
                    break;
            }
            if (!doTransition) {
                throw new StoreException("Invalid index state transition: " + oldState + " -> " + newState);
            }

            HDocumentMutation mutation = new HDocumentMutation().setOrReplace(
                    INDEXES_PATH + "." + indexName + "." + Index.STATE_PATH, newState.toString());
            boolean success = indexCollection.checkAndMutate(tableName.toString(),
                    new HQueryCondition().is(INDEXES_PATH + "." + indexName + "." + Index.STATE_PATH,
                            QueryCondition.Op.EQUAL, oldState.toString()),
                    mutation);
            if (!success) {
                throw new StoreException("Could not update index " + indexName + " to state " + newState);
            }

            // update local cache
            Index index = indexes.get(tableName).get(indexName);
            if (index != null) index.setState(newState);
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    public Map<String, Index> getIndexes(TableName tableName) {
        try {
            Map<String, Index> tableIndexes = indexes.get(tableName);
            return tableIndexes != null ? tableIndexes : Collections.emptyMap();
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    public void dropIndexes(TableName tableName) {
        indexCollection.delete(tableName.toString());
        indexes.invalidate(tableName);
    }

    private Map<String, Index> convertIndexDocument(Document document) {
        Map<String, Index> indexMap = Maps.newHashMap();
        if (document == null) return indexMap;
        Value indexes = document.getValue(FieldPath.parseFrom(INDEXES_PATH));
        if (indexes == null || indexes.getType() == Value.Type.NULL) return indexMap;
        if (indexes.getType() != Value.Type.MAP) {
            throw new StoreException("Invalid indexes document");
        }
        for (Map.Entry<String, Value> entry : (HDocument) indexes) {
            Value indexValue = entry.getValue();
            if (indexValue.getType() != Value.Type.MAP) {
                throw new StoreException("Invalid indexes document");
            }
            indexMap.put(entry.getKey(), new Index((Document) indexValue));
        }
        return indexMap;
    }

    protected void createTable(TableName name) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(name);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY);
        tableDescriptor.addFamily(columnDescriptor);
        connection.getAdmin().createTable(tableDescriptor); // Create the table if not already present
    }

    protected void createTable(TableName name, int maxVersions, boolean keepDeleted) throws IOException {
        HTableDescriptor tableDescriptor = new HTableDescriptor(name);
        HColumnDescriptor columnDescriptor = new HColumnDescriptor(DEFAULT_FAMILY);
        columnDescriptor.setKeepDeletedCells(keepDeleted ? KeepDeletedCells.TRUE : KeepDeletedCells.FALSE);
        columnDescriptor.setMaxVersions(maxVersions);
        tableDescriptor.addFamily(columnDescriptor);
        connection.getAdmin().createTable(tableDescriptor); // Create the table if not already present
    }

    protected boolean tableExists(TableName name) throws IOException {
        return connection.getAdmin().tableExists(name);
    }

    protected Table getTable(TableName name) throws IOException {
        return connection.getTable(name);
    }

    protected void dropTable(TableName name) throws IOException {
        if (tableExists(name)) {
            Admin admin = connection.getAdmin();
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            }
            admin.truncateTable(name, true);
        }
    }

    /*
     * The following methods are for Nashorn integration
     */

    public Set<Entry<String, HDocumentCollection>> entrySet() {
        throw new UnsupportedOperationException();
    }

    public HDocumentCollection get(Object key) {
        return getCollection(key.toString());
    }

}
