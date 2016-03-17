package io.hdocdb.store;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Table;

import java.util.AbstractList;
import java.util.List;

public class IndexQueries extends AbstractList<IndexQuery> {

    private final Table indexTable;
    private final Index index;
    private final List<ConditionRange> ranges;
    private final List<IndexQuery> queries;

    public IndexQueries(Table indexTable, Index index, List<ConditionRange> ranges) {
        this.indexTable = indexTable;
        this.index = index;
        this.ranges = ranges;
        this.queries = Lists.newArrayList();
    }

    public Table getIndexTable() {
        return indexTable;
    }

    public Index getIndex() {
        return index;
    }

    public List<ConditionRange> getRanges() {
        return ranges;
    }

    public List<IndexQuery> getQueries() {
        return queries;
    }

    public IndexQuery get(int index) {
        return queries.get(index);
    }

    public int size() {
        return queries.size();
    }

    public ConditionParent getConditionFromRanges() {
        ConditionParent block = new ConditionParent(ConditionParent.BooleanOp.AND);
        for (ConditionRange range : ranges) {
            block.addAll(range.getConditions());
        }
        return block;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexQueries that = (IndexQueries) o;

        if (!index.equals(that.index)) return false;
        if (!ranges.equals(that.ranges)) return false;
        return queries.equals(that.queries);

    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + ranges.hashCode();
        result = 31 * result + queries.hashCode();
        return result;
    }

}
