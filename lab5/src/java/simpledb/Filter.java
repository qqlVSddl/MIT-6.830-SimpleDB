package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator child;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // done
        predicate = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // done
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // done
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // done
        super.open();
        child.open();
    }

    public void close() {
        // done
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // done
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // done
        while (child.hasNext()) {
            Tuple tp = child.next();
            if (predicate.filter(tp)) {
                return tp;
            }
        }
        return null;
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
