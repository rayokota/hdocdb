package io.hdocdb.compile;

import io.hdocdb.execute.MutationPlan;
import io.hdocdb.execute.QueryIndexPlan;
import io.hdocdb.store.Index;
import io.hdocdb.store.IndexQueries;
import io.hdocdb.store.IndexQuery;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.ojai.store.exceptions.StoreException;

public class DeleteIndexCompiler {

    private Table indexTable;
    private Index index;

    public DeleteIndexCompiler(Table indexTable, Index index) {
        this.indexTable = indexTable;
        this.index = index;
    }

    public MutationPlan compile() throws StoreException {
        try {
            final QueryIndexPlan plan = new QueryIndexCompiler(indexTable, index, null, null).compile();

            return new MutationPlan() {
                public boolean execute() throws StoreException {
                    try {
                        IndexQueries indexQueries = plan.execute();
                        for (IndexQuery indexQuery : indexQueries) {
                            byte[] indexRowKey = Bytes.toBytes(indexQuery.getIndexRowKey());
                            Delete delete = new Delete(indexRowKey, indexQuery.getIndexTs());
                            indexTable.delete(delete);
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
}
