package simpledb;

import java.io.*;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // done
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // done
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // done
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // done
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // done
        if (getId() == pid.getTableId()) {  // same table
            try {
                int pgNo = pid.getPageNumber(), pgSize = BufferPool.getPageSize();
                byte[] buffer = HeapPage.createEmptyPageData();
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                raf.seek((long)pgNo * pgSize);
                raf.read(buffer, 0, pgSize);
                raf.close();
                HeapPageId hpid = (HeapPageId) pid;
                return new HeapPage(hpid, buffer);
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid page, get exception while reading the page.");
            }
        }
        throw new IllegalArgumentException("Page and file belongs to different table");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // done
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        int pgNo = page.getId().getPageNumber();
        raf.seek(BufferPool.getPageSize() * pgNo);
        raf.write(page.getPageData(), 0, BufferPool.getPageSize());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // done
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // done
        // not necessary for lab1
        int pageNum = numPages();
        for (int i = 0; i <= pageNum; ++i) {
            HeapPageId pid = new HeapPageId(getId(), i);
            HeapPage page;
            if (i < pageNum) {
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            } else {
                page = new HeapPage(pid, HeapPage.createEmptyPageData());
            }
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                // only write page when flushPage in bufferpool
                if (i == numPages()) {
                    // To do: why do we have to write the new page to file?
                    // Writing page here can make recovery hard if this transaction is aborted in the future.
                    // Maybe it is OK because it's a new page?
                    writePage(page);
                }
                return new ArrayList<>(Arrays.asList(page));
            }
        }
        throw new DbException("can't insert");
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // done
        // not necessary for lab1
        HeapPageId pid = (HeapPageId) t.getRecordId().getPageId();
        if (pid.getTableId() != getId()) {
            throw new DbException("tuple not a member of this table");
        }
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        page.deleteTuple(t);
        page.markDirty(true, tid);
        return new ArrayList<>(Arrays.asList(page));
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // done

        return new DbFileIterator() {

            private Iterator<Tuple> iter = null;
            private int pageIdx = 0;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                // Let iter be the first page's iterator.
                pageIdx = 0;
                PageId pid = new HeapPageId(getId(), pageIdx);
                Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                HeapPage hPage = (HeapPage) page;
                iter = hPage.iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (iter != null && iter.hasNext()) {
                    return true;
                } else if (pageIdx + 1 < numPages()) {
                    PageId pid = new HeapPageId(getId(), ++pageIdx);
                    Page page = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                    HeapPage hPage = (HeapPage) page;
                    iter = hPage.iterator();
                    return hasNext();
                } else {
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException("no next element");
                }
                return iter.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                iter = null;
                pageIdx = 0;
            }
        };

    }

}

