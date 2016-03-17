package io.hdocdb.compile;

import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.execute.MutationPlan;
import io.hdocdb.store.Index;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.util.Collection;

public class ReplaceCompiler {

    private Table table;
    private Table indexTable;
    private String family;
    private Collection<Index> indexes;
    private HValue id;
    private HDocument newDoc;
    private HDocument oldDoc;

    public ReplaceCompiler(Table table, Table indexTable, String family, Collection<Index> indexes,
                           Value id, Document newDoc, Document oldDoc) {
        this.table = table;
        this.indexTable = indexTable;
        this.family = family;
        this.indexes = indexes;
        this.id = HValue.initFromValue(id);
        this.newDoc = newDoc != null ? HValue.initFromDocument(newDoc) : null;
        this.oldDoc = oldDoc != null ? HValue.initFromDocument(oldDoc) : null;
    }

    public MutationPlan compile() throws StoreException {
        try {
            MutationPlan indexPlan = new InsertIndexCompiler(indexTable, indexes, id, newDoc).compile();
            final RowMutations mutations = constructMutations(id);

            return new MutationPlan() {
                public boolean execute() throws StoreException {
                    try {
                        indexPlan.execute();
                        if (oldDoc != null) {
                            Codec<HValueHolder> codec = new Codec<>();
                            long ts = oldDoc.getTs();
                            if (ts == 0L) {
                                // only check that document already exists (replace)
                                byte[] idBytes = codec.encode(new HValueHolder(id));
                                return table.checkAndMutate(mutations.getRow(), Bytes.toBytes(family),
                                        Bytes.toBytes(HDocument.ID), CompareFilter.CompareOp.EQUAL, idBytes, mutations);
                            } else {
                                // check that document is same as that which satisfied condition
                                // (check and replace)
                                byte[] tsBytes = codec.encode(new HValueHolder(new HValue(new OTimestamp(oldDoc.getTs()))));
                                return table.checkAndMutate(mutations.getRow(), Bytes.toBytes(family),
                                        Bytes.toBytes(HDocument.TS), CompareFilter.CompareOp.EQUAL, tsBytes, mutations);
                            }
                        } else {
                            // insert or replace
                            table.mutateRow(mutations);
                            return true;
                        }
                    } catch (Exception e) {
                        throw new StoreException(e);
                    }
                }
            };
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    private RowMutations constructMutations(HValue id) throws IOException {
        Codec<HValueHolder> codec = new Codec<>();
        byte[] idBytes = codec.encode(new HValueHolder(id));
        RowMutations mutations = new RowMutations(idBytes);
        long now = System.currentTimeMillis();
        // first delete existing row
        Delete delete = new Delete(idBytes, now);
        mutations.add(delete);
        // Set ts to be later than delete
        // See https://issues.apache.org/jira/browse/HBASE-8626
        Put put = new Put(idBytes, now+1);
        id.fillPut(put, family, HDocument.ID_PATH);
        HValue ts = new HValue(new OTimestamp(now));
        ts.fillPut(put, family, HDocument.TS_PATH);
        newDoc.fillPut(put, family, FieldPath.EMPTY);
        mutations.add(put);
        return mutations;
    }

}
