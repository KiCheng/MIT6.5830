package simpledb.execution;

import simpledb.storage.Field;
import simpledb.storage.Tuple;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {
    /**
     * 第一个元组的字段
     */
    private int fieldIndex1;
    /**
     * 第二个元组中的字段
     */
    private int fieldIndex2;

    private Predicate.Op op;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     *
     * @param field1 The field index into the first tuple in the predicate
     * @param field2 The field index into the second tuple in the predicate
     * @param op     The operation to apply (as defined in Predicate.Op); either
     *               Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *               Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *               Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */
    public JoinPredicate(int field1, Predicate.Op op, int field2) {
        // TODO: some code goes here
        this.fieldIndex1 = field1;
        this.fieldIndex2 = field2;
        this.op = op;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     *
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2) {
        // TODO: some code goes here
        if (t1 == null || t2 == null) {
            return false;
        }
        Field field1 = t1.getField(fieldIndex1);
        Field field2 = t2.getField(fieldIndex2);
        return field1.compare(op, field2);
    }

    public int getField1() {
        // TODO: some code goes here
        return fieldIndex1;
    }

    public int getField2() {
        // TODO: some code goes here
        return fieldIndex2;
    }

    public Predicate.Op getOperator() {
        // TODO: some code goes here
        return op;
    }
}
