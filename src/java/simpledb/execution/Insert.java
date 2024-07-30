package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.print.CancelablePrintJob;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;

    private OpIterator[] children;

    private int tableId;

    private TupleDesc tupleDesc;

    /**
     * 需要将插入的结果储存下来，否则会循环调用fetchNext
     */
    private Tuple insertRes;

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // TODO: some code goes here
        this.tid = t;
        this.children = new OpIterator[]{child};
        this.tableId = tableId;

        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"insertNums"});
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        super.open();
        children[0].open();
        insertRes = null;  // 重置插入结果，可以调用fetchNext
    }

    public void close() {
        // TODO: some code goes here
        super.close();
        children[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        this.close();
        this.open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        // 保证只调用一次，多次调用返回null
        if (insertRes != null) {
            return null;
        }
        int insertNums = 0;
        while (children[0].hasNext()) {
            try {
                Database.getBufferPool().insertTuple(tid, tableId, children[0].next());
                insertNums++;
            } catch (IOException e) {
                System.out.println("Insert tuples into database failed!");
                throw new RuntimeException(e);
            }
        }
        insertRes = new Tuple(tupleDesc);  // 计算插入操作影响的行数
        insertRes.setField(0, new IntField(insertNums));
        return insertRes;
    }

    @Override
    public OpIterator[] getChildren() {
        // TODO: some code goes here
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // TODO: some code goes here
        this.children = children;
    }
}
