package simpledb;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator iter;
    private final int tableId;
    private boolean ifInserted;
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // done
        tid = t;
        iter = child;
        this.tableId = tableId;
//        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId))) {
//            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
//            System.out.println(child.getTupleDesc());
//            System.out.println(Database.getCatalog().getTupleDesc(tableId));
//        }
    }

    public TupleDesc getTupleDesc() {
        // done
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // done
        super.open();
        iter.open();
    }

    public void close() {
        // done
        super.close();
        iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // done
        iter.rewind();
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
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // done
        if (!ifInserted) {
            int cnt = 0;
            while (iter.hasNext()) {
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, iter.next());
                    cnt += 1;
                } catch (Exception e) {
                    throw new DbException("IOException in BufferPool's method insertTuple");
                }
            }
            ifInserted = true;
            Tuple res = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            res.setField(0, new IntField(cnt));
            return res;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // done
        return new OpIterator[]{iter};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // done
        iter = children[0];
    }
}
