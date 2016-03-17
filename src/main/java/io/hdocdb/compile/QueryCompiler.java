package io.hdocdb.compile;

import com.google.common.collect.Lists;
import io.hdocdb.execute.QueryIndexPlan;
import io.hdocdb.execute.QueryPlan;
import io.hdocdb.store.ConditionRange;
import io.hdocdb.store.HDocumentFilter;
import io.hdocdb.store.HQueryCondition;
import io.hdocdb.store.Index;
import io.hdocdb.store.IndexQueries;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.ojai.FieldPath;
import org.ojai.store.QueryCondition;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class QueryCompiler {

    private static final Logger LOG = LoggerFactory.getLogger(QueryCompiler.class);

    private Table table;
    private Table indexTable;
    private String family;
    private Collection<Index> indexes;
    private boolean reindexArrays;
    private String indexName;
    private int limit;
    private QueryCondition condition;
    private String[] paths;

    public QueryCompiler(Table table, Table indexTable, String family, Collection<Index> indexes, boolean reindexArrays,
                         String indexName, QueryCondition condition, String... paths) {
        this.table = table;
        this.indexTable = indexTable;
        this.family = family;
        this.indexes = indexes;
        this.reindexArrays = reindexArrays;
        this.indexName = indexName;
        this.limit = -1;
        this.condition = condition;
        this.paths = paths;
    }

    public QueryCompiler(Table table, Table indexTable, String family, Collection<Index> indexes, boolean reindexArrays,
                         String indexName, int limit, QueryCondition condition, String... paths) {
        this.table = table;
        this.indexTable = indexTable;
        this.family = family;
        this.indexes = indexes;
        this.reindexArrays = reindexArrays;
        this.indexName = indexName;
        this.limit = limit;
        this.condition = condition;
        this.paths = paths;
    }

    public QueryPlan compile() throws StoreException {
        try {
            // currently we don't use indexes for projections without conditions
            QueryIndexPlan plan = chooseBestPlan();
            if (plan != null) {
                Index index = plan.getIndex();

                LOG.debug("Using index {}", index.getName());

                IndexQueries indexQueries = plan.execute();
                return new QueryPlan(table, indexQueries, reindexArrays, condition, paths);
            } else {
                Scan scan = constructScan();
                return new QueryPlan(table, scan, reindexArrays, condition, paths);

            }
        } catch (IOException e) {
            throw new StoreException(e);
        }
    }

    private QueryIndexPlan chooseBestPlan() {
        if (Index.NONE.equals(indexName)) return null;

        List<QueryIndexPlan> plans = getIndexPlans();
        if (plans == null) return null;

        Index index = getNamedIndex();
        if (index != null) {
            for (QueryIndexPlan plan : plans) {
                if (plan.getIndex().equals(index)) {
                    return plan;
                }
            }
            // this forces a full scan of the index
            return new QueryIndexCompiler(indexTable, index, condition, paths).compile();
        } else if (plans.isEmpty()) {
            return null;
        } else {
            QueryIndexPlan bestPlan = null;
            for (QueryIndexPlan plan : plans) {
                // choose plan with most matching fields
                if (bestPlan == null || plan.size() > bestPlan.size()) {
                    bestPlan = plan;
                }
            }
            return bestPlan;
        }
    }

    private List<QueryIndexPlan> getIndexPlans() throws IllegalStateException {
        List<QueryIndexPlan> plans = Lists.newArrayList();
        if (condition != null) {
            try {
                Map<FieldPath, ConditionRange> candidateRanges = ((HQueryCondition)condition).getConditionRanges();
                for (Index index : indexes) {
                    if (index.getState() == Index.State.ACTIVE) {
                        QueryIndexPlan plan = new QueryIndexCompiler(
                                indexTable, index, candidateRanges, condition, paths).compile();
                        if (!plan.isEmpty()) plans.add(plan);
                    }
                }
            } catch (Exception e) {
                // getting condition ranges caused an Exception, do a full range scan
                return null;
            }
        }
        return plans;

    }

    private Index getNamedIndex() {
        if (indexName == null) return null;
        for (Index index : indexes) {
            if (index.getName().equals(indexName)) {
                if (index.getState() != Index.State.ACTIVE) {
                    throw new StoreException("Index " + indexName + " is not active");
                }
                return index;
            }
        }
        return null;
    }

    private Scan constructScan() throws IOException {
        Scan scan = new Scan();
        if (limit >= 0) {
            scan.setMaxResultSize(limit);
        }
        if (condition != null || (paths != null && paths.length > 0)) {
            scan.setFilter(new HDocumentFilter(condition, paths));
        }
        return scan;
    }

}
