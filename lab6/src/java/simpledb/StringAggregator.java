package simpledb;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbField;
    private final Type gbFieldType;
    private final int aggField;
    private final Op op;
    private HashMap<String, ArrayList<String>> gbIdxToAggList;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // done
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aggField = afield;
        op = what;
        gbIdxToAggList = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // done
        String key = "NoGrouping";
        if (!(gbField == Aggregator.NO_GROUPING) && gbFieldType == Type.INT_TYPE) {
            key = Integer.toString(((IntField)tup.getField(gbField)).getValue());
        } else if (!(gbField == Aggregator.NO_GROUPING)) {
            key = ((StringField)tup.getField(gbField)).getValue();
        }
        String val = ((StringField)tup.getField(aggField)).getValue();
        if (!gbIdxToAggList.containsKey(key)) {
            gbIdxToAggList.put(key, new ArrayList<>());
        }
        gbIdxToAggList.get(key).add(val);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // done
        // some code goes here
        return new OpIterator() {
            private Iterator iter = null;

            private int calculate(ArrayList<String> list) {
                assert !list.isEmpty() : "expect non empty list";
                int res = 0;
                if (op == Op.COUNT) {
                    return list.size();
                }
                throw new NotImplementedException();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                iter = gbIdxToAggList.entrySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iter == null)
                    throw new IllegalStateException("Operator not yet open");

                return iter.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (iter == null)
                    throw new IllegalStateException("Operator not yet open");

                if (!iter.hasNext()) {
                    throw new NoSuchElementException();
                }

                HashMap.Entry pair = (HashMap.Entry) iter.next();
                ArrayList<String> l = (ArrayList<String>) pair.getValue();
                String key = (String) pair.getKey();
                IntField val = new IntField(calculate(l));

                TupleDesc td = getTupleDesc();
                Tuple tp = new Tuple(td);

                if (gbField == Aggregator.NO_GROUPING) {
                    tp.setField(0, val);
                } else if (gbFieldType == Type.INT_TYPE) {
                    IntField bgFieldValue = new IntField(Integer.parseInt(key));
                    tp.setField(0, bgFieldValue);
                    tp.setField(1, val);
                } else {
                    StringField bgFieldValue = new StringField(key, key.length());
                    tp.setField(0, bgFieldValue);
                    tp.setField(1, val);
                }

                return tp;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                if (gbField == Aggregator.NO_GROUPING) {
                    return new TupleDesc(new Type[]{Type.INT_TYPE});
                }
                return new TupleDesc(new Type[]{gbFieldType, Type.INT_TYPE});
            }

            @Override
            public void close() {
                iter = null;
            }
        };
    }

}
