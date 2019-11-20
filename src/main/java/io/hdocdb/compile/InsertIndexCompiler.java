package io.hdocdb.compile;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.execute.MutationPlan;
import io.hdocdb.store.Index;
import io.hdocdb.store.IndexFieldPath;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.OrderedBytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class InsertIndexCompiler {

    private Table indexTable;
    private Collection<Index> indexes;
    private HValue id;
    private HDocument doc;

    public InsertIndexCompiler(Table indexTable, Collection<Index> indexes, Value id, Document doc) {
        this.indexTable = indexTable;
        this.indexes = indexes;
        this.id = HValue.initFromValue(id);
        this.doc = doc != null ? HValue.initFromDocument(doc) : null;
    }

    public MutationPlan compile() throws StoreException {
        try {
            final List<Put> indexPuts = constructIndexPuts(id);

            return new MutationPlan() {
                public boolean execute() throws StoreException {
                    try {
                        if (!indexPuts.isEmpty()) {
                            Object[] results = new Object[indexPuts.size()];
                            indexTable.batch(indexPuts, results);
                            for (Object result : results) {
                                if (result == null) {
                                    throw new StoreException("Failed to communicate with server");
                                } else if (result instanceof Throwable) {
                                    Throwables.propagate((Throwable) result);
                                }
                            }
                        }
                        return true;
                    } catch (Exception e) {
                        throw new StoreException(e);
                    }
                }
            };
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    private List<Put> constructIndexPuts(HValue id) throws IOException {
        List<Put> batch = Lists.newArrayList();
        if (doc == null) return batch;
        for (Index index : indexes) {
            if (index.getState() != Index.State.INACTIVE && index.getState() != Index.State.DROPPED) {
                Put put = constructIndexPut(index, id);
                if (put != null) batch.add(put);
            }
        }
        return batch;
    }

    private Put constructIndexPut(Index index, HValue id) throws IOException {
        if (doc == null) return null;
        PositionedByteRange indexKey = new SimplePositionedMutableByteRange(4096);
        OrderedBytes.encodeString(indexKey, index.getName(), org.apache.hadoop.hbase.util.Order.ASCENDING);
        for (IndexFieldPath element : index.getFields()) {
            FieldPath fieldPath = element.getPath();
            String path = fieldPath.asPathString();
            HValue value = doc.getHValue(fieldPath);
            // currently we do not support sparse indexes
            if (value == null || value.getType() != element.getType()) {
                value = HValue.NULL;
            }
            OrderedBytes.encodeString(indexKey, path, org.apache.hadoop.hbase.util.Order.ASCENDING);
            value.orderedEncode(indexKey, element.getOrder());
        }

        // encode ID so indexKey is unique
        OrderedBytes.encodeString(indexKey, HDocument.ID, org.apache.hadoop.hbase.util.Order.ASCENDING);
        Codec<HValueHolder> codec = new Codec<>();
        OrderedBytes.encodeBlobCopy(indexKey,
                codec.encode(new HValueHolder(id)),
                org.apache.hadoop.hbase.util.Order.ASCENDING);

        indexKey.setLength(indexKey.getPosition());
        indexKey.setPosition(0);
        byte[] bytes = new byte[indexKey.getRemaining()];
        indexKey.get(bytes);
        long now = System.currentTimeMillis();
        Put put = new Put(bytes, now);  // set timestamp so can be used in deletions
        HValue ts = new HValue(new OTimestamp(now));
        ts.fillPut(put, Index.DEFAULT_FAMILY, HDocument.TS_PATH);
        return put;
    }
}
