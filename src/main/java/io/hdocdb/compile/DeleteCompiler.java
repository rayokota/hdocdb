package io.hdocdb.compile;

import io.hdocdb.HDocument;
import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.execute.MutationPlan;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.Document;
import org.ojai.Value;
import org.ojai.store.exceptions.StoreException;
import org.ojai.types.OTimestamp;

import java.io.IOException;

public class DeleteCompiler {

    private Table table;
    private String family;
    private HValue id;
    private HDocument doc;

    public DeleteCompiler(Table table, String family, Value id, Document doc) {
        this.table = table;
        this.family = family;
        this.id = HValue.initFromValue(id);
        this.doc = doc != null ? HValue.initFromDocument(doc) : null;
    }

    public MutationPlan compile() throws StoreException {
        try {
            final Delete delete = constructDelete();

            return new MutationPlan() {
                public boolean execute() throws StoreException {
                    try {
                        if (doc != null) {
                            Codec<HValueHolder> codec = new Codec<>();
                            byte[] tsBytes = codec.encode(new HValueHolder(new HValue(new OTimestamp(doc.getTs()))));
                            return table.checkAndDelete(delete.getRow(), Bytes.toBytes(family),
                                    Bytes.toBytes(HDocument.TS), CompareFilter.CompareOp.EQUAL, tsBytes, delete);
                        } else {
                            table.delete(delete);
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

    private Delete constructDelete() throws IOException {
        Codec<HValueHolder> codec = new Codec<>();
        byte[] idBytes = codec.encode(new HValueHolder(id));
        Delete delete = new Delete(idBytes);
        return delete;
    }

}
