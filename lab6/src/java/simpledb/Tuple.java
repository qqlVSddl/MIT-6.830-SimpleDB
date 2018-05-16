package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    // my implementation
    private TupleDesc td;
    private RecordId rid;
    private Field[] fields;
//    private

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // my implementation
        this.td = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // my implementation
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // my implementation
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // my implementation
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // my implementation
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // my implementation
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // my implementation
        StringBuilder strBld = new StringBuilder(fields[0].toString());
        for (int i = 1; i < fields.length; ++i) {
            strBld.append("\t" + fields[i].toString());
        }
        return strBld.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // my implementation
        return new Iterator<Field>() {
            int idx = 0;
            @Override
            public boolean hasNext() {
                return idx < fields.length;
            }
            @Override
            public Field next() {
                if (hasNext()) {
                    return fields[idx++];
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // my implementation
        this.td = td;
    }

//
//    /**
//     * Merge two tuples, simply concatenating, for joining.
//     * */
//    public static Tuple merge(Tuple t1, Tuple t2) {
//        TupleDesc mtd = TupleDesc.merge(t1.getTupleDesc(), t2.getTupleDesc());
//        Tuple ret = new Tuple(mtd);
//        ret.setRecordId(null);
//        int i = 0;
//        for (Field f : t1.fields) {
//            ret.setField(i++, f);
//        }
//        for (Field f : t2.fields) {
//            ret.setField(i++, f);
//        }
//        return ret;
//    }
}
