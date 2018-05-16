package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private TDItem[] TDItems;


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // done
        return new Iterator<TDItem>() {
            int idx = 0;
            @Override
            public boolean hasNext() {
                return idx < TDItems.length;
            }

            @Override
            public TDItem next() {
                if (hasNext()) {
                    return TDItems[idx++];
                } else {
                    throw new NoSuchElementException();
                }
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // done
        TDItems = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; ++i) {
            TDItems[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // done
        this(typeAr, new String[typeAr.length]);
    }



    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // done
        return TDItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // done
        try {
            return TDItems[i].fieldName;
        } catch (Exception e) {
            throw new NoSuchElementException("i out of range");
        }
    }


    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // done
        try {
            return TDItems[i].fieldType;
        } catch (Exception e) {
            throw new NoSuchElementException("i out of range");
        }

    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // done
        for (int i = 0; i < TDItems.length; ++i) {
            if (getFieldName(i) != null && getFieldName(i).equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException("No such field name");
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // done
        int sz = 0;
        for (TDItem tdi : TDItems) {
            sz += tdi.fieldType.getLen();
        }
        return sz;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // Done
        Type[] types = new Type[td1.numFields() + td2.numFields()];
        String[] names = new String[td1.numFields() + td2.numFields()];
        int i = 0;
        for (TDItem tdi : td1.TDItems) {
            types[i] = tdi.fieldType;
            names[i++] = tdi.fieldName;
        }
        for (TDItem tdi : td2.TDItems) {
            types[i] = tdi.fieldType;
            names[i++] = tdi.fieldName;
        }
        return new TupleDesc(types, names);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // done
        if (!(o instanceof  TupleDesc)) {
            return false;
        } else {
            TupleDesc tdo = (TupleDesc) o;
            int n = numFields();
            if (tdo.numFields() != n) {
                return false;
            } else {
                for (int i = 0; i < n; ++i) {
                    boolean ifName = tdo.TDItems[i].fieldName == null ?
                            TDItems[i].fieldName == null : tdo.TDItems[i].fieldName.equals(TDItems[i].fieldName);
                    boolean ifType = tdo.TDItems[i].fieldType == TDItems[i].fieldType;
                    if (!ifName || !ifType) {
                        return false;
                    }
                }
                return true;
            }
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }


    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // done
        StringBuilder strBld = new StringBuilder(TDItems[0].toString());
        for (int i = 1; i < TDItems.length; ++i) {
            strBld.append(", " + TDItems[i].toString());
        }
        return strBld.toString();
    }
}
