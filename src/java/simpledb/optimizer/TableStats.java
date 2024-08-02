package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    private final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;

    private ConcurrentHashMap<Integer, IntHistogram> intHistograms;

    private ConcurrentHashMap<Integer, StringHistogram> stringHistograms;

    private HeapFile dbFile;

    private TupleDesc tupleDesc;

    /**
     * 传入表的总记录数，用于估算表基数
     */
    private int totalTuples;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }


    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // TODO: some code goes here
        Map<Integer, Integer> minMap = new HashMap<>();
        Map<Integer, Integer> maxMap = new HashMap<>();
        this.intHistograms = new ConcurrentHashMap<>();
        this.stringHistograms = new ConcurrentHashMap<>();
        this.dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        this.ioCostPerPage = ioCostPerPage;
        this.tupleDesc = dbFile.getTupleDesc();

        Transaction tx = new Transaction();
        tx.start();
        DbFileIterator child = dbFile.iterator(tx.getId());
        try {
            child.open();
            while (child.hasNext()) {
                this.totalTuples += 1;
                Tuple tuple = child.next();
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        // int类型，需要先统计各个属性的最大最小值
                        IntField field = (IntField) tuple.getField(i);
                        // 更新最小值
                        int min_value = Math.min(minMap.getOrDefault(i, Integer.MAX_VALUE), field.getValue());
                        minMap.put(i, min_value);
                        // 更新最大值
                        int max_value = Math.max(maxMap.getOrDefault(i, Integer.MIN_VALUE), field.getValue());
                        maxMap.put(i, max_value);
                    } else if (tupleDesc.getFieldType(i) == Type.STRING_TYPE) {
                        // string类型，直接构造直方图
                        StringHistogram strHis = this.stringHistograms.getOrDefault(i, new StringHistogram(NUM_HIST_BINS));
                        StringField field = (StringField) tuple.getField(i);
                        strHis.addValue(field.getValue());
                        this.stringHistograms.put(i, strHis);
                    }
                }
            }
            // int类型根据最小最大初始化直方图
            for (int i = 0; i < tupleDesc.numFields(); i++) {
                if (minMap.get(i) != null) {
                    // 初始化构造int型直方图
                    this.intHistograms.put(i, new IntHistogram(NUM_HIST_BINS, minMap.get(i), maxMap.get(i)));
                }
            }
            // 重新扫描表，往int直方图添加数据
            child.rewind();
            while (child.hasNext()) {
                Tuple tuple = child.next();
                // 填充直方图的数据
                for (int i = 0; i < tupleDesc.numFields(); i++) {
                    if (tupleDesc.getFieldType(i) == Type.INT_TYPE) {
                        IntField field = (IntField) tuple.getField(i);
                        IntHistogram intHis = this.intHistograms.get(i);
                        if (intHis == null) {
                            throw new IllegalArgumentException("获得直方图失败！");
                        }
                        intHis.addValue(field.getValue());
                        this.intHistograms.put(i, intHis);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            child.close();
            try {
                tx.commit();
            } catch (IOException e) {
                System.out.println("事务提交失败！");
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // TODO: some code goes here
        // 文件页数 * IO代价
        return dbFile.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // TODO: some code goes here
        return (int) (this.totalTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // TODO: some code goes here
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return intHistograms.get(field).avgSelectivity();
        } else if (tupleDesc.getFieldType(field) == Type.STRING_TYPE) {
            return stringHistograms.get(field).avgSelectivity();
        }
        return -1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // TODO: some code goes here
        if (tupleDesc.getFieldType(field) == Type.INT_TYPE) {
            return intHistograms.get(field).estimateSelectivity(op, ((IntField) constant).getValue());
        } else if (tupleDesc.getFieldType(field) == Type.STRING_TYPE) {
            return stringHistograms.get(field).estimateSelectivity(op, ((StringField) constant).getValue());
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // TODO: some code goes here
        return totalTuples;
    }

}
