package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final Page[] buffer;
    private HashMap<PageId, Integer> pageIdToIdx = new HashMap<>();
    private int evictedId = -1;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // done
        buffer = new Page[numPages];
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        int emptyId = -1;
        if (pageIdToIdx.containsKey(pid)) {
            return buffer[pageIdToIdx.get(pid)];
        }
        for (int i = 0; i < buffer.length; ++i) {
            if (null == buffer[i]) {
                emptyId = i;
                break;
            }
        }
        if (emptyId == -1) { // insufficient space in the buffer pool
            evictPage();
            emptyId = evictedId;
//            throw new DbException("in sufficient space");
        }
        buffer[emptyId] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pageIdToIdx.put(pid, emptyId);
        return buffer[emptyId];
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // done
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        ArrayList<Page> list = file.insertTuple(tid, t);
        for (Page p : list) {
            p.markDirty(true, tid);
            PageId pid = p.getId();
            if (tableId != pid.getTableId()) {
                throw new DbException("invalid page in the dirty array");
            }
            if (!pageIdToIdx.containsKey(pid)) {
                try {
                    file.readPage(pid);
                } catch (Exception e) {
                    System.out.println(1);
                    file.writePage(p);
                }
                getPage(tid, pid, Permissions.READ_WRITE);
//                System.out.println(buffer[pageIdToIdx.get(pid)].getId().getPageNumber() + " " + p.getId().getPageNumber());
            }
//            if (pageIdToIdx.containsKey(pid)) {
//                int idx = pageIdToIdx.get(pid);
//                buffer[idx] = p;
//            } else {
//                System.out.println(pid);
//            }
            buffer[pageIdToIdx.get(pid)] = p;
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // done
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> list = file.deleteTuple(tid, t);
        for (Page p : list) {
            p.markDirty(true, tid);
            buffer[pageIdToIdx.get(p.getId())] = p;
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // done
        // not necessary for lab1
        for (PageId pid : pageIdToIdx.keySet()) {
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // done
        // not necessary for lab1
        buffer[pageIdToIdx.get(pid)] = null;
        pageIdToIdx.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // done
        // not necessary for lab1
        if (pageIdToIdx.containsKey(pid)) {
            Page page = buffer[pageIdToIdx.get(pid)];
            if (page.isDirty() != null) {
                page.markDirty(false, null);
                Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            }
        }


    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // done
        // not necessary for lab1
        // For simplity, just delete the "first" page
        PageId pid = (PageId) pageIdToIdx.keySet().toArray()[0];
        int idx = pageIdToIdx.get(pid);
        try {
            flushPage(pid);
        } catch (Exception e) {
            throw new DbException("error when flushing");
        }
        discardPage(pid);
        evictedId = idx;
    }

}
