# Lecture Notes

Reference:
http://db.lcs.mit.edu/6.830/lectures/lec12-notes.pdf

## Transaction

#### ACID:
 - Atomicity: all or nothing
 - Consistency: guarantee certain invariants
 - Isolation: concurrently running actions can't see each other's partial results
 - Durability: completed actions remain in effect even after a crash.

Essential properties: atomic and recoverable (return to consistent state after crash)

#### Serializability:
We interleave the execution, but have the end result as though those concurrent actions had run in some serial order.

**View serializability**: S is view equivalent to a serial ordering S' iff:
- Every value read in S is the same value that was read by the same read in S'.
- Final write of every object is done by same transaction T in S and S'.

**Conflict serializability**: It is possible to swap non-conflicting operations to derive a serial schedule. This means that for all pairs of conflicting operations {O1 in T1, O2 in T2} either O1 always precedes O2, or O2 always precedes O1.
(Conflicting actions: 2 operations, if you perform them in different order, you get different result.)
T<sub>i</sub> -> T<sub>j</sub> if: T<sub>i</sub> reads/writes some A before T<sub>j</sub> writes A, or T<sub>i</sub> writes some A before T<sub>j</sub> reads. If the graph is circle free, we have a conflict serializable schedule.

Note: some view serializability isn't conflict serializability. (blind writes, overwrite by another T.)

#### Recoverability:

**Cascading rollback**:  If T2 reads something written by T1, T1 has commited before T2 performs that read. Cascading is recoverable.


### Locking

Shared Lock for reading; Exclusive lock for writing. Slock can be upgraded to XLock.

#### Two Phase Locking

**Two Phase:**
Growing: acquire locks; Shrinking: release locks.
The end of growing phase: lock point. Serial order between conflicting transactions is determined by lock points.

2PL locking isn't cascadeless.
Ex: T1 writes A, realeases lock on A, and then aborts. T2 reads A which is written by T1. In this case, T2 has to rollback too because it reads T1's dirty data.

#### **Strict Two Phase Locking**

Don't release write locks until transactions commits.
Rigorous two phase locking holds read locks as well. It is conflict serializable and cascadeless. Also, we often can't tell when xaction is going to end.

**Protocol**:
- Before every read, acquire a shared lock
- Before every write, acquire an exclusive lock (or "upgrade") a shared to an
exclusive lock
- Release all locks after the transaction commits


**DeadLock**

- Detect deadlocks, by looking for cycles in its wait-for graph (A waits B, B waits A). Then shoot one of the transaction.
- Timeout policy: aborts a transaction if it has not completed after a given period of time.


# Implementation Notes

## Java Concurrency

Reference: https://docs.oracle.com/javase/tutorial/essential/concurrency/index.html

### Thread

Thread exist within a process. They share the process's resources, including memory and open files. This makes threads efficient.

Thread.sleep or wait: cause the current thread to suspend execution for a specified period. This makes processor time available to the other threads of an aplication. This may cause InterruptedException. Both methods work for this lab.
Difference between Thread.sleep and wait: https://stackoverflow.com/questions/1036754/difference-between-wait-and-sleep

### Synchronization

Threads communicate primarily by sharing access to fields and the objects reference fields refer to. This can cause problem (thread interference, memory consistency error). So we need synchronization.

**Synchronized Method:** 
- Not possible for two invacations of synchronized methods on the same object to interleave.
- Guarantees that changes to the state of the object by one thread is visible to all threads.
 
**Intrinsic Locks and Synchronization:** 
Synchronized statements are useful for improving concurrency with fine-grained synchronization. In this lab, we implement page-level lock. So we only need to acquire an intrinsic lock on PageId.


### Concurrent HashMap
https://docs.oracle.com/javase/8/docs/api/java/util/concurrent/ConcurrentHashMap.html
Change the previous buffer data structure in this lab.