package io.hdocdb.compile;

import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.execute.MutationPlan;
import io.hdocdb.store.Index;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.util.Collection;

public class InsertCompiler {

    private Table table;
    private Table indexTable;
    private String family;
    private Collection<Index> indexes;
    private HValue id;
    private HDocument doc;

    public InsertCompiler(Table table, Table indexTable, String family, Collection<Index> indexes,
                          Value id, Document doc) {
        this.table = table;
        this.indexTable = indexTable;
        this.family = family;
        this.indexes = indexes;
        this.id = HValue.initFromValue(id);
        this.doc = doc != null ? HValue.initFromDocument(doc) : null;
    }

    public MutationPlan compile() throws StoreException {
        try {
            MutationPlan indexPlan = new InsertIndexCompiler(indexTable, indexes, id, doc).compile();
            final Put put = constructPut(id);

            return new MutationPlan() {
                public boolean execute() throws StoreException {
                    try {
                        indexPlan.execute();
                        return table.checkAndPut(put.getRow(), Bytes.toBytes(family), Bytes.toBytes(HDocument.ID), null,  put);
                    } catch (IOException e) {
                        throw new StoreException(e);
                    }
                }
            };
        } catch (Exception e) {
            throw new StoreException(e);
        }
    }

    private Put constructPut(HValue id) throws IOException {
        Codec<HValueHolder> codec = new Codec<>();
        byte[] idBytes = codec.encode(new HValueHolder(id));
        Put put = new Put(idBytes);
        id.fillPut(put, family, HDocument.ID_PATH);
        long now = System.currentTimeMillis();
        HValue ts = new HValue(new OTimestamp(now));
        ts.fillPut(put, family, HDocument.TS_PATH);
        doc.fillPut(put, family, FieldPath.EMPTY);
        return put;
    }

}
