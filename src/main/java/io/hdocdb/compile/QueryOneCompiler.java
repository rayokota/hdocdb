package io.hdocdb.compile;

import io.hdocdb.HValue;
import io.hdocdb.HValueHolder;
import io.hdocdb.execute.QueryPlan;
import io.hdocdb.store.HDocumentFilter;
import io.hdocdb.util.Codec;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.Value;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;

import java.io.IOException;

public class QueryOneCompiler {

    private Table table;
    private String family;
    private boolean reindexArrays;
    private HValue id;
    private QueryCondition condition;
    private String[] paths;

    public QueryOneCompiler(Table table, String family,
                            boolean reindexArrays, Value id, QueryCondition condition, String... paths) {
        this.table = table;
        this.family = family;
        this.reindexArrays = reindexArrays;
        this.id = HValue.initFromValue(id);
        this.condition = condition;
        this.paths = paths;
    }

    public QueryPlan compile() throws StoreException {
        try {
            return new QueryPlan(table, constructGet(), reindexArrays, condition, paths);
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    private Get constructGet() throws IOException {
        Codec<HValueHolder> codec = new Codec<>();
        byte[] idBytes = codec.encode(new HValueHolder(id));
        Get get = new Get(idBytes);
        if (condition != null || (paths != null && paths.length > 0)) {
            get.setFilter(new HDocumentFilter(condition, paths));
        }
        return get;
    }

}
