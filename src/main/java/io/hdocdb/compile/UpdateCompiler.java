package io.hdocdb.compile;

import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.execute.MutationPlan;
import io.hdocdb.store.HDocumentMutation;
import io.hdocdb.store.Index;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.Value;
import org.ojai.store.DocumentMutation;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.OTimestamp;

import java.io.IOException;
import java.util.Collection;

public class UpdateCompiler {

    private Table table;
    private Table indexTable;
    private String family;
    private Collection<Index> indexes;
    private HValue id;
    private DocumentMutation mutation;
    private HDocument doc;

    public UpdateCompiler(Table table, Table indexTable, String family, Collection<Index> indexes,
                          Value id, DocumentMutation m, Document doc) {
        this.table = table;
        this.indexTable = indexTable;
        this.family = family;
        this.indexes = indexes;
        this.id = HValue.initFromValue(id);
        this.mutation = m;
        this.doc = doc != null ? HValue.initFromDocument(doc) : null;
    }

    public MutationPlan compile() throws StoreException {
        try {
            HDocument sourceDoc = doc != null ? doc : ((HDocumentMutation)mutation).asDocument();
            MutationPlan indexPlan = new InsertIndexCompiler(indexTable, indexes, id, sourceDoc).compile();
            final RowMutations mutations = constructMutations(id);

            return new MutationPlan() {
                public boolean execute() throws StoreException {
                    try {
                        indexPlan.execute();
                        if (doc != null) {
                            Codec<HValueHolder> codec = new Codec<>();
                            byte[] tsBytes = codec.encode(new HValueHolder(new HValue(new OTimestamp(doc.getTs()))));
                            return table.checkAndMutate(mutations.getRow(), Bytes.toBytes(family),
                                    Bytes.toBytes(HDocument.TS), CompareFilter.CompareOp.EQUAL, tsBytes, mutations);
                        } else {
                            table.mutateRow(mutations);
                            return true;
                        }
                    } catch (IOException e) {
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
        Put put = new Put(idBytes);
        long now = System.currentTimeMillis();
        HValue ts = new HValue(new OTimestamp(now));
        ts.fillPut(put, family, HDocument.TS_PATH);
        mutations.add(put);
        ((HDocumentMutation)mutation).fillMutations(mutations, family, doc);
        return mutations;
    }

}
