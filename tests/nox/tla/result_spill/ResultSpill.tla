-------------------------- MODULE ResultSpill --------------------------
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS Pages, MaxInterrupts, DeadlineReads, KeepUntilRestored, BatchFullPages,
          QueueFirstAfterTimeout
VARIABLES p, queue, tail, disk, layouts, stopped, r, delivered, lastPage,
          publishedDuringFallback
vars == <<p, queue, tail, disk, layouts, stopped, r, delivered, lastPage,
          publishedDuringFallback>>
Ids == 1..Pages
Entries == [id : Ids, kind : {"memory", "spill"}]
SeqIds(s) == {s[i] : i \in 1..Len(s)}
QueueIds == {queue[i].id : i \in 1..Len(queue)}
MemoryPages == Cardinality({i \in 1..Len(queue) : queue[i].kind = "memory"})
OneOrEmpty(n) == IF n = 0 THEN {} ELSE {n}

Init ==
    /\ p = [next |-> 1, pending |-> 0, stage |-> "idle"]
    /\ queue = <<>> /\ tail = 0 /\ disk = {} /\ layouts = {}
    /\ stopped = FALSE
    /\ r = [pc |-> "idle", page |-> 0, interrupts |-> 0, cancelledPage |-> 0,
             fromFallback |-> FALSE]
    /\ delivered = <<>> /\ lastPage = 0
    /\ publishedDuringFallback = FALSE

\* One full page per producer input; the last input is a partial page.
\* Capacity is one full in-memory page; spilled pages count as zero.
BufferMemory ==
    /\ ~BatchFullPages
    /\ p.stage = "idle" /\ p.next < Pages /\ MemoryPages = 0
    /\ queue' = Append(queue, [id |-> p.next, kind |-> "memory"])
    /\ p' = [p EXCEPT !.next = @ + 1]
    /\ UNCHANGED <<tail, disk, layouts, stopped, r, delivered, lastPage,
                    publishedDuringFallback>>

\* A single input containing two full pages enters try_add_block's loop:
\* first page queued and second detached under ONE buffer lock. This case
\* matches a 9-row block with page_rows=max_rows=4 (partial tail follows).
BufferBatch ==
    /\ BatchFullPages /\ Pages = 3
    /\ p.stage = "idle" /\ p.next = 1
    /\ queue' = <<[id |-> 1, kind |-> "memory"]>>
    /\ p' = [next |-> 3, pending |-> 2, stage |-> "writing"]
    /\ UNCHANGED <<tail, disk, layouts, stopped, r, delivered, lastPage,
                    publishedDuringFallback>>

DetachForSpill ==
    /\ p.stage = "idle" /\ p.next < Pages /\ MemoryPages = 1
    /\ p' = [p EXCEPT !.pending = p.next, !.next = @ + 1, !.stage = "writing"]
    /\ UNCHANGED <<queue, tail, disk, layouts, stopped, r, delivered, lastPage,
                    publishedDuringFallback>>

\* The write await completes before layout registration and queue publication.
SpillWritten ==
    /\ p.stage = "writing"
    /\ disk' = disk \cup {p.pending}
    /\ p' = [p EXCEPT !.stage = "written"]
    /\ UNCHANGED <<queue, tail, layouts, stopped, r, delivered, lastPage,
                    publishedDuringFallback>>

RegisterLayout ==
    /\ p.stage = "written"
    /\ layouts' = layouts \cup {p.pending}
    /\ p' = [p EXCEPT !.stage = "registered"]
    /\ UNCHANGED <<queue, tail, disk, stopped, r, delivered, lastPage,
                    publishedDuringFallback>>

PublishSpill ==
    /\ p.stage = "registered"
    /\ queue' = Append(queue, [id |-> p.pending, kind |-> "spill"])
    /\ p' = [p EXCEPT !.pending = 0, !.stage = "idle"]
    /\ publishedDuringFallback' = (publishedDuringFallback \/ r.pc = "fallback")
    /\ UNCHANGED <<tail, disk, layouts, stopped, r, delivered, lastPage>>

BufferTail ==
    /\ p.stage = "idle" /\ p.next = Pages /\ tail = 0
    /\ tail' = p.next
    /\ p' = [p EXCEPT !.next = @ + 1]
    /\ UNCHANGED <<queue, disk, layouts, stopped, r, delivered, lastPage,
                    publishedDuringFallback>>

Finish ==
    /\ p.stage = "idle" /\ p.next = Pages + 1
    /\ queue' = IF tail = 0 THEN queue
                 ELSE Append(queue, [id |-> tail, kind |-> "memory"])
    /\ tail' = 0 /\ stopped' = TRUE
    /\ p' = [p EXCEPT !.stage = "done"]
    /\ UNCHANGED <<disk, layouts, r, delivered, lastPage, publishedDuringFallback>>

ObserveEmpty ==
    /\ r.pc = "idle" /\ Len(queue) = 0 /\ ~stopped
    /\ r' = [r EXCEPT !.pc = "waiting"]
    /\ UNCHANGED <<p, queue, tail, disk, layouts, stopped, delivered, lastPage,
                    publishedDuringFallback>>

\* timeout polls recv first. Expiry is admitted only with an empty queue.
\* Another OS worker can run after expiry, before take_current_page locks.
DeadlineExpired ==
    /\ DeadlineReads /\ r.pc = "waiting" /\ Len(queue) = 0 /\ ~stopped
    /\ r' = [r EXCEPT !.pc = "fallback"]
    /\ UNCHANGED <<p, queue, tail, disk, layouts, stopped, delivered, lastPage,
                    publishedDuringFallback>>

\* The fixed fallback selects queued pages first under the SAME buffer lock.
\* Mutation QueueFirstAfterTimeout=FALSE restores the old unchecked take.
TakePartialAfterTimeout ==
    /\ r.pc = "fallback"
    /\ (~QueueFirstAfterTimeout \/ Len(queue) = 0)
    /\ delivered' = IF tail = 0 THEN delivered ELSE Append(delivered, tail)
    /\ lastPage' = IF tail = 0 THEN lastPage ELSE tail
    /\ tail' = 0
    /\ r' = [r EXCEPT !.pc = "idle"]
    /\ UNCHANGED <<p, queue, disk, layouts, stopped, publishedDuringFallback>>

TakeMemory ==
    /\ (r.pc \in {"idle", "waiting"}
        \/ (QueueFirstAfterTimeout /\ r.pc = "fallback"))
    /\ Len(queue) > 0
    /\ Head(queue).kind = "memory"
    /\ delivered' = Append(delivered, Head(queue).id)
    /\ lastPage' = Head(queue).id /\ queue' = Tail(queue)
    /\ r' = [r EXCEPT !.pc = "idle"]
    /\ UNCHANGED <<p, tail, disk, layouts, stopped, publishedDuringFallback>>

BeginRestore ==
    /\ (r.pc \in {"idle", "waiting"}
        \/ (QueueFirstAfterTimeout /\ r.pc = "fallback"))
    /\ Len(queue) > 0
    /\ Head(queue).kind = "spill"
    /\ r' = [r EXCEPT !.pc = "restore", !.page = Head(queue).id,
                       !.fromFallback = (r.pc = "fallback")]
    /\ queue' = IF KeepUntilRestored THEN queue ELSE Tail(queue)
    /\ UNCHANGED <<p, tail, disk, layouts, stopped, delivered, lastPage,
                    publishedDuringFallback>>

\* Dropping a future or returning a read error must not consume the page.
InterruptRestore(cancelled) ==
    /\ r.pc = "restore" /\ r.interrupts < MaxInterrupts
    /\ r' = [r EXCEPT !.pc = "idle", !.page = 0, !.interrupts = @ + 1,
                       !.cancelledPage = IF cancelled THEN r.page ELSE @]
    /\ UNCHANGED <<p, queue, tail, disk, layouts, stopped, delivered, lastPage,
                    publishedDuringFallback>>
CancelRestore == InterruptRestore(TRUE)
FailRestore == InterruptRestore(FALSE)

\* No await between successful restore, commit, serialization and last-page
\* caching. Single receiver prevents a second pop during this suffix.
RestoreAndCommit ==
    /\ r.pc = "restore" /\ r.page \in disk \cap layouts
    /\ (IF KeepUntilRestored THEN
           Len(queue) > 0 /\ Head(queue).id = r.page
        ELSE TRUE)
    /\ queue' = IF KeepUntilRestored THEN Tail(queue) ELSE queue
    /\ delivered' = Append(delivered, r.page) /\ lastPage' = r.page
    /\ r' = [r EXCEPT !.pc = "idle", !.page = 0]
    /\ UNCHANGED <<p, tail, disk, layouts, stopped, publishedDuringFallback>>

Next == BufferMemory \/ BufferBatch \/ DetachForSpill \/ SpillWritten \/ RegisterLayout
        \/ PublishSpill \/ BufferTail \/ Finish \/ ObserveEmpty \/ DeadlineExpired
        \/ TakePartialAfterTimeout \/ TakeMemory \/ BeginRestore
        \/ CancelRestore \/ FailRestore \/ RestoreAndCommit

TypeOK ==
    /\ p \in [next : 1..(Pages + 1), pending : 0..Pages,
              stage : {"idle", "writing", "written", "registered", "done"}]
    /\ queue \in Seq(Entries) /\ Len(queue) <= Pages
    /\ tail \in 0..Pages /\ disk \subseteq Ids /\ layouts \subseteq Ids
    /\ stopped \in BOOLEAN
    /\ r \in [pc : {"idle", "waiting", "fallback", "restore"},
              page : 0..Pages, interrupts : 0..MaxInterrupts,
              cancelledPage : 0..Pages, fromFallback : BOOLEAN]
    /\ delivered \in Seq(Ids) /\ Len(delivered) <= Pages
    /\ lastPage \in 0..Pages /\ publishedDuringFallback \in BOOLEAN

NoPageLoss ==
    Ids = SeqIds(delivered) \cup QueueIds \cup OneOrEmpty(tail)
          \cup OneOrEmpty(p.pending) \cup (p.next..Pages)
          \cup (IF r.pc = "restore" THEN {r.page} ELSE {})
OrderedDelivery == \A i \in 1..Len(delivered) : delivered[i] = i
NoDuplicates == Cardinality(SeqIds(delivered)) = Len(delivered)
ReadableSpills == \A i \in 1..Len(queue) :
    queue[i].kind = "spill" => queue[i].id \in disk \cap layouts
CacheMatchesLast == lastPage = IF Len(delivered) = 0 THEN 0 ELSE delivered[Len(delivered)]
NeverCancelledRetry == ~(r.cancelledPage \in SeqIds(delivered))
NeverSpillDuringFallback == ~publishedDuringFallback
NeverFallbackRestoreWithTail ==
    ~(r.pc = "restore" /\ r.fromFallback /\ tail # 0)
=============================================================================
