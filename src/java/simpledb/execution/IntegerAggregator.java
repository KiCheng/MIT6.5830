package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUP_FIELD = new StringField("NO_GROUP_FIELD", 20);

    /**
     * 需要分组的字段索引(从0开始)
     */
    private int groupByIndex;

    /**
     * 需要分组的字段类型
     */
    private Type groupByType;

    /**
     * 聚合字段索引(从0开始)
     */
    private int aggregateIndex;

    /**
     * 聚合操作
     */
    private Op aggOp;

    /**
     * 分组计算Map
     */
    private Map<Field, GroupCalResult> groupCalMap;
    private Map<Field, Tuple> resultMap;

    /**
     * for groupCalMap
     */
    private static class GroupCalResult {
        public static final Integer DEFAULT_COUNT = 0;
        public static final Integer Deactivate_COUNT = -1;
        public static final Integer DEFAULT_RES = 0;
        public static final Integer Deactivate_RES = -1;
        /**
         * 当前分组计算的结果: sum, avg, max, min, count
         */
        private Integer result;

        /**
         * 当前Field出现的频率
         */
        private Integer count;

        public GroupCalResult(int result , int count) {
            this.result = result;
            this.count = count;
        }
    }

    /**
     * 聚合后Tuple的desc
     */
    private TupleDesc aggDesc;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        this.groupByIndex = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateIndex = afield;
        this.aggOp = what;

        // init map
        this.groupCalMap = new ConcurrentHashMap<>();
        this.resultMap = new ConcurrentHashMap<>();

        if (groupByIndex >= 0) {
            // 有groupBy
            this.aggDesc = new TupleDesc(new Type[]{groupByType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        } else {
            // 无groupBy
            this.aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field groupByField = this.groupByIndex == NO_GROUPING ? NO_GROUP_FIELD : tup.getField(groupByIndex);
        // 分组字段类型检查
        if(!NO_GROUP_FIELD.equals(groupByField) && groupByField.getType() != groupByType) {
            throw new IllegalArgumentException("Expected groupByType: 「" + groupByType + "」, but got: " + groupByField.getType());
        }
        // 聚合字段类型检查
        if(!(tup.getField(aggregateIndex) instanceof IntField)) {
            throw new IllegalArgumentException("Expected aggType is 「IntField」, but got: " + tup.getField(aggregateIndex).getClass());
        }
        IntField aggField = (IntField)tup.getField(aggregateIndex);
        int curVal = aggField.getValue();

        // 1、store
        GroupCalResult groupCalResult = null;
        switch (this.aggOp) {
            // 实现SUM AVG MIN MAX COUNT
            case MIN:
                groupCalResult = new GroupCalResult(
                        Math.min(groupCalMap.getOrDefault(groupByField, new GroupCalResult(Integer.MAX_VALUE, GroupCalResult.Deactivate_COUNT)).result, curVal),
                        GroupCalResult.Deactivate_COUNT
                );
                groupCalMap.put(groupByField, groupCalResult);
                break;
            case MAX:
                groupCalResult = new GroupCalResult(
                        Math.max(groupCalMap.getOrDefault(groupByField, new GroupCalResult(Integer.MIN_VALUE, GroupCalResult.Deactivate_COUNT)).result, curVal),
                        GroupCalResult.Deactivate_COUNT
                );
                groupCalMap.put(groupByField, groupCalResult);
                break;
            case SUM:
                groupCalResult = new GroupCalResult(
                        groupCalMap.getOrDefault(groupByField, new GroupCalResult(GroupCalResult.DEFAULT_RES, GroupCalResult.Deactivate_COUNT)).result + curVal,
                        GroupCalResult.Deactivate_COUNT
                );
                groupCalMap.put(groupByField, groupCalResult);
                break;
            case COUNT:
                groupCalResult = new GroupCalResult(
                        GroupCalResult.Deactivate_RES,
                        groupCalMap.getOrDefault(groupByField, new GroupCalResult(GroupCalResult.Deactivate_RES, GroupCalResult.DEFAULT_COUNT)).count + 1
                );
                groupCalMap.put(groupByField, groupCalResult);
                break;
            case AVG:
                // 上一个分组的结果
                GroupCalResult pre = groupCalMap.getOrDefault(groupByField, new GroupCalResult(GroupCalResult.DEFAULT_RES, GroupCalResult.DEFAULT_COUNT));
                groupCalResult = new GroupCalResult(pre.result + curVal, pre.count + 1);
                groupCalMap.put(groupByField, groupCalResult);
                break;
        }
        // 2、cal
        Tuple curCalTuple = new Tuple(aggDesc);
        int curRes = 0;
        if(aggOp == Op.MIN || aggOp == Op.MAX || aggOp == Op.SUM) {
            curRes = groupCalMap.get(groupByField).result;
        } else if (aggOp == Op.COUNT) {
            curRes = groupCalMap.get(groupByField).count;
        } else if (aggOp == Op.AVG) {
            curRes = groupCalMap.get(groupByField).result / groupCalMap.get(groupByField).count;
        }
        if (groupByIndex >= 0) {
            curCalTuple.setField(0, groupByField);
            curCalTuple.setField(1, new IntField(curRes));
        } else {
            curCalTuple.setField(0, new IntField(curRes));
        }
        // 3、update
        resultMap.put(groupByField, curCalTuple);

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return new IntAggTupIterator();
    }

    private class IntAggTupIterator implements OpIterator {
        private boolean open = false;
        private Iterator<Map.Entry<Field, Tuple>> iter;

        @Override
        public void open() {
            iter = resultMap.entrySet().iterator();
            open = true;
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public boolean hasNext() {
            return open && iter.hasNext();
        }

        @Override
        public Tuple next() {
            return iter.next().getValue();
        }

        @Override
        public void rewind() {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggDesc;
        }
    }

}
