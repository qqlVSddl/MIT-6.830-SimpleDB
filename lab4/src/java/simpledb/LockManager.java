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
                Random rand = new Random();
//                Thread.sleep(rand.nextInt(40) + 1);
//                Thread.sleep(10);
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
                    pageIdToLock.get(pid).type = LockType.XLock;
                    return true;
                }
            } else if (ifHoldsLock(tid, pid)) {
                return true;
            } else {    // other SLock or XLock exist.
                return false;
            }
        }
        return false;
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            assert !ifHoldsLock(tid, pid) : "try to release unexisting lock";
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

    public HashSet<PageId> getLockedPage(TransactionId tid) {
        return tidToLockedPage.getOrDefault(tid, null);
    }
}
