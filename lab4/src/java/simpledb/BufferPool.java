package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
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

    private ConcurrentHashMap<PageId, Page> buffer;
    private int capacity;
    private LockManager lockManager;


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
        lockManager = new LockManager();
        buffer = new ConcurrentHashMap<>();
        capacity = numPages;
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

        // acquire the lock
        lockManager.acquireLock(tid, pid, perm);

        if (buffer.containsKey(pid)) { // already in buffer
            return buffer.get(pid);
        }

        if (buffer.size() >= capacity) {   // buffer is full, need to evict page
            evictPage();
        }

        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        buffer.put(pid, page);

        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     * (my understanding: when a transaction doesn't use any data of a page.
     * For example, a transaction scans page for empty slot to insert tuple, but can't find one.
     * It will immediately release the SLock on this page.)
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.ifHoldsLock(tid, p);
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
        if (commit) {
            flushPages(tid);
        } else {    // abort the transaction by discarding all pages dirtied by this transaction.
            HashSet<PageId> lockedList = lockManager.getLockedPage(tid);
            if (lockedList == null) {   // this transaction may acquire no lock, have no need to releaseLock too.
                return;
            }
            for (PageId pid : lockedList) {
                if (!buffer.containsKey(pid)) {
                    continue;
                }
                Page pg = buffer.get(pid);
                if (pg.isDirty() != null) { // dirty page
                    // The locking system ensures that this page can only be dirtied by tid.
                    // discard this dirty page
                    discardPage(pid);
                }
            }
        }
        lockManager.releaseLock(tid);
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
            if (!buffer.containsKey(pid) && buffer.size() == capacity) {
                evictPage();
            }
            buffer.put(pid, p);
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
            getPage(tid, p.getId(), Permissions.READ_WRITE);
            buffer.put(p.getId(), p);
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
        for (PageId pid : buffer.keySet()) {
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
        if (!buffer.containsKey(pid)) {
            // To do: this happens a lot. Figure out why
            System.out.println("discarding non-existing page");
            return;
        }
        buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // done
        // not necessary for lab1
        if (buffer.containsKey(pid)) {
            Page page = buffer.get(pid);
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
        HashSet<PageId> pageIds = lockManager.getLockedPage(tid);
        if (pageIds == null) {  // tid may acquire no lock
            return;
        }
        for (PageId pid : pageIds) {
            flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * (Flushes the page to disk to ensure dirty pages are updated on disk.)
     * Implementing NO STEAL: Modifications from a transaction are written to disk only after it commits.
     * This means we can abort a transaction by discarding the dirty pages and rereading them from disk.
     * Thus, we must not evict dirty pages, otherwise, we may abort a transaction.
     */
    private synchronized  void evictPage() throws DbException {
        // done
        // not necessary for lab1
        // For simplicity, just delete the "first" non-dirty page
        Enumeration<PageId> iter =  buffer.keys();
        while (iter.hasMoreElements()) {
            PageId pid = iter.nextElement();
            if (buffer.get(pid).isDirty() == null) {   // non dirty page, evict it
                discardPage(pid);
                return;
            }
        }

        throw new DbException("BufferPool: evictPage: all dirty");
    }

}
