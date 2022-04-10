package io.hdocdb.store;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.hdocdb.HValue;
import org.ojai.FieldPath;

import java.util.List;

public class ConditionRange {

    private FieldPath field;
    private Range<HValue> range;
    private List<ConditionLeaf> conditions;

    public ConditionRange(FieldPath field, Range<HValue> range, ConditionLeaf condition) {
        this.field = field;
        this.range = range;
        this.conditions = Lists.newArrayList();
        this.conditions.add(condition);
    }

    public ConditionRange(FieldPath field, Range<HValue> range, List<ConditionLeaf> conditions) {
        this.field = field;
        this.range = range;
        this.conditions = conditions;
    }

    public FieldPath getField() {
        return field;
    }

    public void setField(FieldPath field) {
        this.field = field;
    }

    public Range<HValue> getRange() {
        return range;
    }

    public void setRange(Range<HValue> range) {
        this.range = range;
    }

    public boolean isSingleton() {
        return range.hasLowerBound() && range.equals(Range.singleton(range.lowerEndpoint()));
    }

    public List<ConditionLeaf> getConditions() {
        return conditions;
    }

    public void addCondition(ConditionLeaf condition) {
        conditions.add(condition);
    }

    public void setConditions(List<ConditionLeaf> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConditionRange that = (ConditionRange) o;

        if (!field.equals(that.field)) return false;
        if (!range.equals(that.range)) return false;
        return conditions.equals(that.conditions);

    }

    @Override
    public int hashCode() {
        int result = field.hashCode();
        result = 31 * result + range.hashCode();
        result = 31 * result + conditions.hashCode();
        return result;
    }

    public ConditionRange merge(ConditionRange that) {
        if (!that.getField().equals(getField())) throw new IllegalArgumentException("Range fields don't match");
        List<ConditionLeaf> conditions = Lists.newArrayList(getConditions());
        conditions.addAll(that.getConditions());
        return new ConditionRange(getField(), getRange().span(that.getRange()), conditions);
    }

}
