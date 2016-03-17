package io.hdocdb.compile;

import com.google.common.collect.Lists;
import io.hdocdb.execute.QueryIndexPlan;
import io.hdocdb.store.ConditionRange;
import io.hdocdb.store.Index;
import io.hdocdb.store.IndexFieldPath;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.FieldPath;
import org.ojai.store.QueryCondition;

import java.util.List;
import java.util.Map;

public class QueryIndexCompiler {

    private Table indexTable;
    private Index index;
    private List<ConditionRange> ranges;
    private QueryCondition condition;
    private String[] paths;

    public QueryIndexCompiler(Table indexTable, Index index, QueryCondition c, String... paths) {
        this.indexTable = indexTable;
        this.index = index;
        this.ranges = Lists.newArrayList();
        this.condition = c;
        this.paths = paths;
    }

    public QueryIndexCompiler(Table indexTable, Index index, Map<FieldPath, ConditionRange> candidateRanges,
                              QueryCondition c, String... paths) {
        this.indexTable = indexTable;
        this.index = index;
        this.ranges = Lists.newArrayList();
        for (IndexFieldPath element : index.getFields()) {
            ConditionRange range = candidateRanges.get(element.getPath());
            if (range != null) {
                this.ranges.add(range);
            } else {
                // candidate ranges must match a left prefix of the index
                // (it may be possible to relax this in the future)
                break;
            }
        }
        this.condition = c;
        this.paths = paths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueryIndexCompiler indexPlan = (QueryIndexCompiler) o;

        if (!index.equals(indexPlan.index)) return false;
        return ranges.equals(indexPlan.ranges);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + ranges.hashCode();
        return result;
    }

    public QueryIndexPlan compile() {
        return new QueryIndexPlan(indexTable, new Scan(), index, ranges, condition, paths);
    }
}
