package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * 直方图中的桶，记录每个桶的高度
     */
    private int[] buckets;

    /**
     * 直方图的最大值
     */
    private int max;

    /**
     * 直方图的最小值
     */
    private int min;

    /**
     * 直方图的桶宽度
     */
    private double width;

    /**
     * 直方图的元组数
     */
    private int tupleCount;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // TODO: some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max;
        this.width = (double) (max - min + 1) / buckets;
        this.tupleCount = 0;
    }

    /**
     * 根据value值获取桶的序号
     */
    private int getIndex(int value) {
        int index = (int) ((value - min) / width);
        if (index < 0 || index >= buckets.length) {
            return -1;
        }
        return (int) ((value - min) / width);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // TODO: some code goes here
        if (v >= min && v <= max) {
            int index = getIndex(v);
            buckets[index]++;
            tupleCount++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        // TODO: some code goes here
        switch (op) {
            case LESS_THAN:
                if (v <= min) {
                    return 0.0;
                } else if (v >= max) {
                    return 1.0;
                } else {
                    int index = getIndex(v);
                    double tuples = 0;
                    for (int i = 0; i < index; i++) {
                        tuples += buckets[i];
                    }
                    tuples += (buckets[index] / width) * (v - (min + index * width));  // add 直方图index桶最后一部分
                    return tuples / tupleCount;
                }
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            case EQUALS:
                return estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v) - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN, v - 1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            default:
                throw new UnsupportedOperationException("Unsupported operation: " + op);
        }
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // TODO: some code goes here
        double sum = 0;
        for (int bucket: buckets) {
            sum += ((1.0 * bucket) / tupleCount);
        }
        return sum / buckets.length;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // TODO: some code goes here
        return "IntHistogram{" +
                "buckets=" + Arrays.toString(buckets) +
                ", max=" + max +
                ", min=" + min +
                ", width=" + width +
                ", tupleCount=" + tupleCount +
                "}";
    }
}
