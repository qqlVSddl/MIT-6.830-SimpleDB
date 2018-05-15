package simpledb;

import java.util.ArrayList;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int bucketNum;
    private final int min;
    private final int max;
    private final double bucketSize;
    private int ntups = 0;
    private int[] buckets;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// done
        this.bucketNum = buckets;
        this.max = max;
        this.min = min;
        this.bucketSize = (double)(max - min) / bucketNum;
//        System.out.println(max + " " + min + " " + bucketSize);
        this.buckets = new int[buckets];
        for (int i = 0; i < buckets; ++i) {
            this.buckets[i] = 0;
        }
    }

    public int getIdx(int v) {
        assert v >= min && v <= max : "invalid value, out of range" + min + " " + max + " " + v;
//        System.out.println((int)((v - min) / bucketSize) + " " + bucketNum + " " + bucketSize);
        return v == max ? bucketNum - 1 : (int)((v - min) / bucketSize);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// done
//        System.out.println(getIdx(v));
//        int idx = getIdx(v);
//        System.out.println(idx);
        ++buckets[getIdx(v)];
        ++ntups;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// done
        switch (op) {
            case EQUALS:
                if (v < min || v > max) {
                    return 0.;
                }
                return 1.0 * buckets[getIdx(v)] / ((int)bucketSize+1) / ntups;
            case LESS_THAN:
                if (v <= min) {
                    return 0.;
                } else if (v >= max) {
                    return 1.;
                }
                int idx = getIdx(v), cnt = 0;
                for (int i = 0; i < idx; ++i) {
                    cnt += buckets[i];
                }
                return (cnt + (v - (min + idx * bucketSize)) / bucketSize * buckets[idx]) / ntups;
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case GREATER_THAN:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v + 1);
            case GREATER_THAN_OR_EQ:
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            case NOT_EQUALS:
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v );
        }
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // done

        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // done
        String res = "";
        for (int i = 0; i < bucketNum; ++i) {
            res += "bucket" + i + ": " + buckets[i] + "\n";
        }
        return res;
    }
}
