# Lecture Notes

Reference
http://db.lcs.mit.edu/6.830/lectures/lec14-notes.pdf

**Recovery**
- Ensure atomicity by giving us a way to roll back aborted xactions.
- ensure durability: eg, committed xactions actually appear on storage after a crash

Buffer pool in the memory; Tables and log in the disk. After crash, memory is gone. Recovery is about restoring disk to a "transaction consistent" state.

### Log Based Recovery

Write log records before write any update to disk
Log records for a xaction must be on disk before you can commit.

**Log info** contains
1. What you plan to do -> log records
2. Whether you did it or not -> current on state disk

**Records in log:**
- SOT: transaction id
- EOT: tid/commit or abort
- UNDO: before image or logical update (undo)
- REDO: after image or logical update (redo)
- CHECKPOINT: current state which allow us to limit how much we have to undo/redo
- CLR: allow us to restart recovery

**Steal vs No Steal:**

No Steal: no need to undo (never write dirty pages to disk).
Steal: high throughput.

**Force vs No Force:**

Force: Modified pages are always written to disk before the commit record. No need to redo.
No Force: high throughput, especially when a page is modified by many transactions.

Unlike simpledb in lab5, most commercial database do !FORCE/STEAL for performance reason.

Determine what to undo from loser (with SOT and not EOT) records in the log.
Deterimine what to redo by checking most recent update applied to winner transactions identified in the log scan.

