package simpledb;

//import sun.plugin.javascript.navig.Array;

import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Random;

/**
 * LockManager manages lock
 */

public class LockManager {
    public enum LockType {
        SLock, XLock, NoLock
    }

    /**
     * inner class, page-level lock
     */
    class Lock {
        private HashSet<TransactionId> tidSet;
        private LockType type;

        Lock(TransactionId tid, LockManager.LockType t) {
            tidSet = new HashSet<>(Arrays.asList(tid));
            type = t;
        }

        public synchronized void addTid(TransactionId tid) {
            tidSet.add(tid);
        }
    }

    private ConcurrentHashMap<PageId, Lock> pageIdToLock;
    private ConcurrentHashMap<TransactionId, HashSet<PageId>> tidToLockedPage;
    private final int waitTime = 500;

    LockManager() {
        pageIdToLock = new ConcurrentHashMap<>();
        tidToLockedPage = new ConcurrentHashMap<>();
    }

    public LockType getLockType(PageId pid) {
        synchronized (pid) {
            if (!pageIdToLock.containsKey(pid)) {
                return LockType.NoLock;
            }
            return pageIdToLock.get(pid).type;
        }
    }

    /**
     * @return if tid holds a lock on pid
     * */
    public boolean ifHoldsLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            return pageIdToLock.containsKey(pid) && pageIdToLock.get(pid).tidSet.contains(tid);
        }
    }

    /**
     * tid adds a lock on pid
     *
     *
     * @param tid
     * @param pid
     * @param perm
     *            decides the type of lock
     */
    public void acquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException {
        long start = System.currentTimeMillis();
        while (!(perm == Permissions.READ_ONLY ? acquireSLock(tid, pid) : acquireXLock(tid, pid))) {
            try {
                // Release intrinsic lock on pid
                // Give other threads chance to release Slock or XLock on pid.
                // Wait random time to stagger the threads
                Random rand = new Random();
                wait(rand.nextInt(40) + 1);
            } catch (Exception e) {
                // do nothing
            }
            if (System.currentTimeMillis() - start > waitTime) {
                // Out of time. Abort this transaction.
                System.out.println("aborted");
                throw new TransactionAbortedException();
            }
        }
        // lock successfully, update tidToLockedPage
        // No need to synchronize on this part, cuz won't exist 2 threads on same tid.
        if (tidToLockedPage.containsKey(tid)) {
            tidToLockedPage.get(tid).add(pid);
        } else {
            tidToLockedPage.put(tid, new HashSet<>(Arrays.asList(pid)));
        }
    }

    private boolean acquireSLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            if (ifHoldsLock(tid, pid)) {
                return true;
            }
            LockType lockState = getLockType(pid);
            if (lockState == LockType.NoLock) {
                pageIdToLock.put(pid, new Lock(tid, LockType.SLock));
                return true;
            } else if (lockState == LockType.SLock) {
                pageIdToLock.get(pid).addTid(tid);
                return true;
            } else {
                return false;
            }
        }
    }

    private boolean acquireXLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            LockType lockState = getLockType(pid);
            if (lockState == LockType.NoLock) {
                pageIdToLock.put(pid, new Lock(tid, LockType.XLock));
                return true;
            } else if (lockState == LockType.SLock) {
                if (ifHoldsLock(tid, pid) && pageIdToLock.get(pid).tidSet.size() == 1) {
                    // upgrade the SLock
                    pageIdToLock.get(pid).type = LockType.XLock;
                    return true;
                }
            } else if (ifHoldsLock(tid, pid)) { // tid already holds an XLock on pid
                return true;
            }
        }
        // other transaction holds SLock or XLock on pid
        return false;
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            if (!ifHoldsLock(tid, pid)) {
                return;
            }
            pageIdToLock.get(pid).tidSet.remove(tid);
            if (pageIdToLock.get(pid).tidSet.isEmpty()) {
                pageIdToLock.remove(pid);
            }
            tidToLockedPage.get(tid).remove(pid);
            if (tidToLockedPage.get(tid).isEmpty()) {
                tidToLockedPage.remove(tid);
            }
        }
    }

    public void releaseLock(TransactionId tid) {
        if (tidToLockedPage.containsKey(tid)) {
            HashSet<PageId> pageIds = tidToLockedPage.get(tid);
            for (PageId pid : pageIds) {
                synchronized (pid) {
                    pageIdToLock.get(pid).tidSet.remove(tid);
                    if (pageIdToLock.get(pid).tidSet.isEmpty()) {
                        pageIdToLock.remove(pid);
                    }
                }
            }
            tidToLockedPage.remove(tid);
        }
    }

    /*
     * Return the pages which are locked by tid
     * If tid holds no lock on any page, return null
     */
    public HashSet<PageId> getLockedPage(TransactionId tid) {
        return tidToLockedPage.getOrDefault(tid, null);
    }
}
