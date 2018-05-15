package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private final TransactionId tid;
    private OpIterator iter;
    private Boolean ifDeleted = false;
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // done
        tid = t;
        iter = child;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (!ifDeleted) {
            int cnt = 0;
            while (iter.hasNext()) {
                try {
                    Database.getBufferPool().deleteTuple(tid, iter.next());
                    cnt += 1;
                } catch (TransactionAbortedException e) {
                    System.out.println("delete aborted");
                    throw e;
                } catch (Exception e) {
                    System.out.println(e);
                    throw new DbException("IO exception in deleteTuple in Bufferpool");
                }
            }
            ifDeleted = true;
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
