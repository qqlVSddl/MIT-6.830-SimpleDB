package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator child, iter;
    private final int aggField, gbField;
    private final Aggregator.Op op;
    private final Aggregator aggregator;
    Type gbFieldType, aggFieldType;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // done
        this.child = child;
        aggField = afield;
        gbField = gfield;
        op = aop;
        gbFieldType = null;
        if (gbField != Aggregator.NO_GROUPING) {
//            System.out.println(child.getTupleDesc().numFields());
//            System.out.printf("%d\t %d\n", child.getTupleDesc().numFields(),gbField);
            gbFieldType = child.getTupleDesc().getFieldType(gbField);
        }
        aggFieldType = child.getTupleDesc().getFieldType(aggField);
        if (aggFieldType == Type.INT_TYPE) {
            aggregator = new IntegerAggregator(gbField, gbFieldType, aggField, op);
        } else {
            aggregator = new StringAggregator(gbField, gbFieldType, aggField, op);
        }
        iter = null;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // done
	    return gbField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	    // done
	    return getTupleDesc().getFieldName(gbField);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // done
        return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // done
        return getTupleDesc().getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // done
        return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // done
        super.open();
        child.open();
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        child.close();
        iter = aggregator.iterator();
        iter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // done
        if (iter.hasNext()) {
            return iter.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // done
        iter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // done
        String aggName = child.getTupleDesc().getFieldName(aggField), aname;
        if (aggName == null) {
            aname = null;
        } else {
            aname = new String(aggName + "(" + op.toString() + ")");
        }
        if (gbField == Aggregator.NO_GROUPING) {
            return new TupleDesc(new Type[]{aggFieldType},
                                 new String[]{aname});
        }

        String gbName = child.getTupleDesc().getFieldName(gbField), gname;
        if (gbName == null) {
            gname = null;
        } else {
            gname = gbName + "(" + op.toString() + ")";
        }
        return new TupleDesc(new Type[]{gbFieldType, aggFieldType},
                             new String[]{gname, aname});
    }

    public void close() {
	    // done
        super.close();
        iter.close();
    }

    @Override
    public OpIterator[] getChildren() {
	    // done
	    return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
	    // done
        child = children[0];
    }
    
}
