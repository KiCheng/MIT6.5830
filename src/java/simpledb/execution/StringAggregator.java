package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

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
     * 聚合的字段结构
     */
    private TupleDesc aggDesc;

    /**
     * 分组计算Map
     */
    private Map<Field, Integer> groupCalMap;  // 只需要计算count
    private Map<Field, Tuple> resultMap;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // TODO: some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("Only support 「COUNT」");
        }
        this.groupByIndex = gbfield;
        this.groupByType = gbfieldtype;
        this.aggregateIndex = afield;

        if (groupByIndex >= 0) {
            this.aggDesc = new TupleDesc(new Type[]{groupByType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        } else {
            this.aggDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        }

        this.groupCalMap = new ConcurrentHashMap<>();
        this.resultMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // TODO: some code goes here
        Field groupField = groupByIndex >= 0 ? tup.getField(groupByIndex) : NO_GROUP_FIELD;
        // 分组字段类型检查
        if (!NO_GROUP_FIELD.equals(groupField) && groupField.getType() != groupByType) {
            throw new IllegalArgumentException("Expected groupByType: 「" + groupByType + "」, but got: " + groupField.getType());
        }
        // 聚合字段类型检查
        if (!(tup.getField(aggregateIndex) instanceof StringField)) {
            throw new IllegalArgumentException("Expected aggregateType: 「" + Type.STRING_TYPE + "」, but got: " + tup.getField(aggregateIndex).getType());
        }
        // 1、store
        groupCalMap.put(groupField, this.groupCalMap.getOrDefault(groupField, 0) + 1);
        // 2、cal
        Tuple curCalTuple = new Tuple(aggDesc);
        if (groupByIndex >= 0) {
            curCalTuple.setField(0, groupField);
            curCalTuple.setField(1, new IntField(groupCalMap.get(groupField)));
        } else {
            curCalTuple.setField(0, new IntField(groupCalMap.get(groupField)));
        }
        // 3、update
        resultMap.put(groupField, curCalTuple);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // TODO: some code goes here
        return new StringAggTupIterator();
    }

    private class StringAggTupIterator implements OpIterator {

        private boolean open = false;
        private Iterator<Map.Entry<Field, Tuple>> iter;

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iter = resultMap.entrySet().iterator();
            open = true;
        }

        @Override
        public void close() {
            open = false;
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return open && iter.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iter.next().getValue();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.close();
            this.open();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggDesc;
        }
    }

}
