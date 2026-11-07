------------------------- MODULE CloneVacuumCommit -------------------------
\* Copyright 2026 Datafuse Labs
\* SPDX-License-Identifier: Apache-2.0
\* Scope and source mapping: model.json. Bounded checks, not a Rust proof.
EXTENDS Naturals, FiniteSets

CONSTANTS InitiallyGrouped, Statements, Delta, CheckRetryTime,
          RescanClones, PinStagedSnapshot, KeepEarliestFence, CloneCacheHits,
          RetainSnapshots, OverwriteFirst, AdvanceAppends, SplitAdvance, CheckAdvanceSeq
ASSUME /\ InitiallyGrouped \in BOOLEAN
       /\ Statements \in {1, 2}
       /\ Delta \in {0, 2}
       /\ CheckRetryTime \in BOOLEAN
       /\ RescanClones \in BOOLEAN
       /\ PinStagedSnapshot \in BOOLEAN
       /\ KeepEarliestFence \in BOOLEAN
       /\ CloneCacheHits \in BOOLEAN
       /\ RetainSnapshots \in {1, 3}
       /\ OverwriteFirst \in BOOLEAN /\ AdvanceAppends \in BOOLEAN
       /\ SplitAdvance \in BOOLEAN /\ CheckAdvanceSeq \in BOOLEAN

Snaps == {"s0", "sx", "w1", "w2", "r1", "r2", "anchor"}
SourceSnaps == Snaps \ {"anchor"}
Segs == {"g0", "gx", "g1", "g2"}
Blocks == {"b0", "bx", "b1", "b2"}
BlockOf == [g \in Segs |-> CASE g = "g0" -> "b0" [] g = "gx" -> "bx"
                           [] g = "g1" -> "b1" [] OTHER -> "b2"]
BlocksOf(gs) == {BlockOf[g] : g \in gs}
Max(a, b) == IF a > b THEN a ELSE b
Min(a, b) == IF a < b THEN a ELSE b
NoFence == 99
MaxRetries == 2
MaxCloneRefreshes == 3

VARIABLES source, store, writer, clone, gc, clock, advanced, pendingFirst
vars == <<source, store, writer, clone, gc, clock, advanced, pendingFirst>>

\* store retains immutable object CONTENTS after deletion, for modeling reads
\* already in memory. Only the three live sets describe physical existence.
Init ==
    /\ source = [seq |-> 0, head |-> "s0", grouped |-> InitiallyGrouped,
                  lvt |-> 0, lvtSeq |-> 0]
    /\ store = [snapshots |-> {"s0"}, segments |-> {"g0"}, blocks |-> {"b0"},
                 times |-> [s \in Snaps |-> 0],
                 refs |-> [s \in Snaps |-> IF s = "s0" THEN {"g0"} ELSE {}],
                 prev |-> [s \in Snaps |-> "none"],
                 born |-> [o \in Segs \cup Blocks |-> 0]]
    /\ writer = [phase |-> "idle", base |-> "s0", pending |-> "s0",
                  count |-> 0, seq |-> 0, viewGrouped |-> FALSE,
                  statementTime |-> 0, blockTime |-> 0, begin |-> 0,
                  fence |-> NoFence, fenceSeq |-> 0, retries |-> 0,
                  retryTime |-> 0, retryBlockTime |-> 0, latest |-> "s0",
                  cache |-> {}]
    /\ clone = [phase |-> "idle", selected |-> "s0", head |-> "s0",
                 cursor |-> "s0", target |-> "choose", refreshes |-> 0,
                 cachedSelection |-> FALSE,
                 sourceSeq |-> 0, fenceSeq |-> 0, bound |-> FALSE,
                 lvt |-> 0, lvtSeq |-> 0, failed |-> FALSE,
                 firstDuringWrite |-> FALSE]
    /\ gc = [phase |-> "idle", fenceSeq |-> 0, root |-> "s0", cutoff |-> 0,
              listedSnaps |-> {}, anchor |-> "s0",
              protectedSegs |-> {}, protectedSnaps |-> {}, protectedBlocks |-> {},
              snapCandidates |-> {}, segCandidates |-> {}, blockCandidates |-> {},
              sawClone |-> FALSE, readHead |-> "s0", cloneFenceSeq |-> 0,
              touchClone |-> FALSE, cloneFenceTime |-> 0]
    /\ clock = 1
    /\ advanced = [phase |-> "idle", base |-> "s0", seq |-> 0,
                    time |-> 0, grouped |-> FALSE, fenceSeq |-> 0]
    /\ pendingFirst \in BOOLEAN

Refs(s) == store.refs[s]
Time(s) == store.times[s]
WriterSegs == IF writer.count = 1 THEN {"g1"} ELSE {"g1", "g2"}
RemovedSegs == IF OverwriteFirst THEN Refs(writer.base) ELSE {}
ReadableByWriter(s) == s \in store.snapshots \/ s \in writer.cache
\* Cache mode represents a remote reader retaining previously committed history.
\* Navigation ONLY starts from a captured committed head and follows prev links.
ReadableByClone(s) == s \in store.snapshots \/ CloneCacheHits
SnapshotRank(s) == CASE s = "s0" -> 0 [] s = "sx" -> 3
                       [] s = "w1" -> IF pendingFirst THEN 1 ELSE 4
                       [] s = "w2" -> IF pendingFirst THEN 2 ELSE 5
                       [] s = "r1" -> 6 [] s = "r2" -> 7 [] OTHER -> 8
KeyBefore(a, b) == Time(a) < Time(b)
                   \/ (Time(a) = Time(b) /\ SnapshotRank(a) < SnapshotRank(b))

\* Read committed TableInfo once. Later statements use the buffered TableInfo,
\* NOT the current source's group flag or sequence (SessionCatalog::get_table).
WriterBegin ==
    /\ writer.phase = "idle"
    /\ source.head \in store.snapshots
    /\ writer' = [writer EXCEPT !.phase = "statement", !.base = source.head,
          !.pending = source.head, !.seq = source.seq,
          !.viewGrouped = source.grouped, !.cache = {source.head}]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

WriterSampleTime ==
    /\ writer.phase = "statement"
    /\ LET t == Max(clock, Time(writer.pending) + 1)
       IN writer' = [writer EXCEPT !.phase = "writeBlock",
                        !.statementTime = t, !.blockTime = t + Delta,
                        !.begin = IF writer.count = 0 THEN t + Delta ELSE @]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

WriterWriteBlock ==
    /\ writer.phase = "writeBlock"
    /\ LET b == IF writer.count = 0 THEN "b1" ELSE "b2"
       IN store' = [store EXCEPT !.blocks = @ \cup {b}, !.born[b] = writer.blockTime]
    /\ writer' = [writer EXCEPT !.phase = "writeSegment"]
    /\ UNCHANGED <<source, clone, gc, clock, advanced, pendingFirst>>

WriterWriteSegment ==
    /\ writer.phase = "writeSegment"
    /\ LET g == IF writer.count = 0 THEN "g1" ELSE "g2"
       IN store' = [store EXCEPT !.segments = @ \cup {g}, !.born[g] = writer.blockTime]
    /\ writer' = [writer EXCEPT !.phase = "writeSnapshot"]
    /\ UNCHANGED <<source, clone, gc, clock, advanced, pendingFirst>>

WriterWriteSnapshot ==
    /\ writer.phase = "writeSnapshot"
    /\ LET s == IF writer.count = 0 THEN "w1" ELSE "w2"
           g == IF writer.count = 0 THEN "g1" ELSE "g2"
           t == Max(writer.statementTime, Time(writer.pending) + 1)
       IN IF writer.blockTime < t
          THEN /\ writer' = [writer EXCEPT !.phase = "aborted"]
               /\ UNCHANGED store
          ELSE /\ store' = [store EXCEPT !.snapshots = @ \cup {s}, !.times[s] = t,
                               !.refs[s] = (IF OverwriteFirst /\ writer.count = 0
                                            THEN {} ELSE Refs(writer.pending)) \cup {g},
                               \* decorate_snapshot skips earlier buffered statements.
                               !.prev[s] = writer.base]
               /\ writer' = [writer EXCEPT !.phase = "buffer", !.pending = s]
    /\ UNCHANGED <<source, clone, gc, clock, advanced, pendingFirst>>

\* update_multi_table_meta buffers first; the successful update populates cache.
WriterBuffer ==
    /\ writer.phase = "buffer"
    /\ writer' = [writer EXCEPT !.phase = "buffered", !.count = @ + 1,
          !.cache = @ \cup {writer.pending},
          !.fence = IF writer.viewGrouped
                    THEN IF KeepEarliestFence THEN Min(@, Time(writer.pending))
                         ELSE Time(writer.pending)
                    ELSE @]
    /\ UNCHANGED store
    /\ UNCHANGED <<source, clone, gc, clock, advanced, pendingFirst>>

WriterNextStatement ==
    /\ writer.phase = "buffered"
    /\ writer.count < Statements
    /\ writer' = [writer EXCEPT !.phase = "statement"]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

\* COMMIT reads base and transaction snapshots, not all data blocks. They were
\* cached by this writer; allow cache hits after physical GC, like FuseCaches.
WriterLoadDiff ==
    /\ writer.phase = "buffered"
    /\ writer.count = Statements
    /\ writer' = [writer EXCEPT !.phase =
          IF ReadableByWriter(writer.base) /\ ReadableByWriter(writer.pending)
          THEN "ready" ELSE "aborted"]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

WriterPrecheck ==
    /\ writer.phase = "ready"
    /\ IF writer.seq # source.seq
       THEN writer' = [writer EXCEPT !.phase = "retry",
                         !.seq = source.seq, !.latest = source.head]
       ELSE writer' = [writer EXCEPT !.phase =
              IF writer.fence # NoFence /\ writer.fence < source.lvt
              THEN "aborted" ELSE "checked", !.fenceSeq = source.lvtSeq]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

WriterCAS ==
    /\ writer.phase = "checked"
    /\ IF writer.seq = source.seq
          /\ (writer.fence = NoFence \/ writer.fenceSeq = source.lvtSeq)
       THEN /\ source' = [source EXCEPT !.head = writer.pending, !.seq = @ + 1]
            /\ writer' = [writer EXCEPT !.phase = "committed"]
       ELSE /\ writer' = [writer EXCEPT !.phase = "retry",
                            !.seq = source.seq, !.latest = source.head]
            /\ UNCHANGED source
    /\ UNCHANGED <<store, clone, gc, clock, advanced, pendingFirst>>

\* Retry uses the metadata returned by the failed Meta transaction, not a
\* fresh head. That snapshot may already be stale when its bytes are loaded.
WriterRetryRead ==
    /\ writer.phase = "retry"
    /\ IF writer.retries = MaxRetries
          \/ ~ReadableByWriter(writer.latest) \/ ~ReadableByWriter(writer.base)
          \/ ~(RemovedSegs \subseteq Refs(writer.latest))
          \/ (CheckRetryTime /\ writer.begin < Time(writer.latest))
       THEN writer' = [writer EXCEPT !.phase = "aborted"]
       ELSE writer' = [writer EXCEPT !.phase = "retryTime",
                         !.cache = @ \cup {writer.latest}]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

\* generate_table_stats awaits IO between the guard and timestamp sampling.
WriterRetryTime ==
    /\ writer.phase = "retryTime"
    /\ LET t == Max(clock, Time(writer.latest) + 1)
       IN writer' = [writer EXCEPT !.phase = "rebuild",
            !.retryTime = IF writer.retries = 0 THEN t ELSE @,
            !.retryBlockTime = IF writer.retries = 0 THEN t + Delta ELSE @]
    /\ UNCHANGED <<source, store, clone, gc, clock, advanced, pendingFirst>>

WriterRebuild ==
    /\ writer.phase = "rebuild"
    /\ LET s == IF writer.retries = 0 THEN "r1" ELSE "r2"
           t == Max(writer.retryTime, Time(writer.latest) + 1)
       IN IF writer.retryBlockTime < t
          THEN /\ writer' = [writer EXCEPT !.phase = "aborted"]
               /\ UNCHANGED store
          ELSE /\ store' = [store EXCEPT !.snapshots = @ \cup {s}, !.times[s] = t,
                               !.refs[s] = (Refs(writer.latest) \ RemovedSegs) \cup WriterSegs,
                               !.prev[s] = writer.latest]
               /\ writer' = [writer EXCEPT !.phase = "ready", !.pending = s,
                                !.retries = @ + 1, !.cache = @ \cup {s}]
    /\ UNCHANGED <<source, clone, gc, clock, advanced, pendingFirst>>

\* Keep the previous atomic environment for baseline cases. SplitAdvance cases
\* expose this same writer's IO and CAS windows instead (no retry after conflict).
AdvanceSource ==
    /\ ~SplitAdvance /\ advanced.phase = "idle"
    /\ LET t == Max(clock + 2, Time(source.head) + 1)
       IN /\ source' = [source EXCEPT !.head = "sx", !.seq = @ + 1]
          /\ store' = [store EXCEPT !.snapshots = @ \cup {"sx"},
                 !.segments = @ \cup {"gx"}, !.blocks = @ \cup {"bx"},
                 !.times["sx"] = t,
                 !.refs["sx"] = {"gx"} \cup (IF AdvanceAppends THEN Refs(source.head) ELSE {}),
                 !.prev["sx"] = source.head,
                 !.born["gx"] = t + Delta, !.born["bx"] = t + Delta]
    /\ clock' = clock + 2
    /\ advanced' = [advanced EXCEPT !.phase = "committed"]
    /\ UNCHANGED <<writer, clone, gc, pendingFirst>>

\* Capture TableInfo, then read its snapshot separately. This actor uses physical
\* reads; once sampled, immutable snapshot contents remain in memory across IO.
AdvanceRead ==
    /\ SplitAdvance /\ advanced.phase = "idle"
    /\ advanced' = [advanced EXCEPT !.phase = "sample", !.base = source.head,
                        !.seq = source.seq, !.grouped = source.grouped]
    /\ UNCHANGED <<source, store, writer, clone, gc, clock, pendingFirst>>

AdvanceSampleTime ==
    /\ advanced.phase = "sample"
    /\ IF advanced.base \notin store.snapshots
       THEN /\ advanced' = [advanced EXCEPT !.phase = "aborted"]
            /\ UNCHANGED clock
       ELSE /\ advanced' = [advanced EXCEPT !.phase = "writeBlock",
                     !.time = Max(clock + 2, Time(advanced.base) + 1)]
            /\ clock' = clock + 2
    /\ UNCHANGED <<source, store, writer, clone, gc, pendingFirst>>

AdvanceWriteBlock ==
    /\ advanced.phase = "writeBlock"
    /\ store' = [store EXCEPT !.blocks = @ \cup {"bx"},
                     !.born["bx"] = advanced.time + Delta]
    /\ advanced' = [advanced EXCEPT !.phase = "writeSegment"]
    /\ UNCHANGED <<source, writer, clone, gc, clock, pendingFirst>>

AdvanceWriteSegment ==
    /\ advanced.phase = "writeSegment"
    /\ store' = [store EXCEPT !.segments = @ \cup {"gx"},
                     !.born["gx"] = advanced.time + Delta]
    /\ advanced' = [advanced EXCEPT !.phase = "writeSnapshot"]
    /\ UNCHANGED <<source, writer, clone, gc, clock, pendingFirst>>

AdvanceWriteSnapshot ==
    /\ advanced.phase = "writeSnapshot"
    /\ store' = [store EXCEPT !.snapshots = @ \cup {"sx"},
          !.times["sx"] = advanced.time, !.prev["sx"] = advanced.base,
          !.refs["sx"] = {"gx"} \cup (IF AdvanceAppends THEN Refs(advanced.base) ELSE {})]
    /\ advanced' = [advanced EXCEPT !.phase = "ready"]
    /\ UNCHANGED <<source, writer, clone, gc, clock, pendingFirst>>

AdvancePrecheck ==
    /\ advanced.phase = "ready"
    /\ advanced' = [advanced EXCEPT !.phase =
          IF CheckAdvanceSeq /\ advanced.seq # source.seq THEN "conflict"
          ELSE IF advanced.grouped /\ advanced.time < source.lvt THEN "aborted"
          ELSE "checked", !.fenceSeq = source.lvtSeq]
    /\ UNCHANGED <<source, store, writer, clone, gc, clock, pendingFirst>>

AdvanceCAS ==
    /\ advanced.phase = "checked"
    /\ IF (~CheckAdvanceSeq \/ advanced.seq = source.seq)
          /\ (~advanced.grouped \/ advanced.fenceSeq = source.lvtSeq)
       THEN /\ source' = [source EXCEPT !.head = "sx", !.seq = @ + 1]
            /\ advanced' = [advanced EXCEPT !.phase = "committed"]
       ELSE /\ advanced' = [advanced EXCEPT !.phase = "conflict"]
            /\ UNCHANGED source
    /\ UNCHANGED <<store, writer, clone, gc, clock, pendingFirst>>

CloneReadSource ==
    /\ clone.phase \in {"idle", "refresh"}
    /\ clone' = [clone EXCEPT !.phase = "navigate", !.cursor = source.head,
                    !.sourceSeq = source.seq]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

\* find_snapshot_with_version walks committed ancestry, not a directory listing.
\* Earlier buffered statements have files but decorate_snapshot keeps them out.
CloneNavigate ==
    /\ clone.phase = "navigate"
    /\ IF ~ReadableByClone(clone.cursor)
       THEN clone' = [clone EXCEPT !.phase = "aborted"]
       ELSE \/ /\ clone.target \in {"choose", clone.cursor}
               /\ clone' = [clone EXCEPT !.phase = "readSnapshot",
                     !.selected = clone.cursor, !.target = clone.cursor,
                     !.cachedSelection = (clone.cursor \notin store.snapshots)]
            \/ /\ store.prev[clone.cursor] # "none"
               /\ clone.cursor # clone.target
               /\ clone' = [clone EXCEPT !.cursor = store.prev[clone.cursor]]
            \/ /\ store.prev[clone.cursor] = "none"
               /\ clone.target \notin {"choose", clone.cursor}
               /\ clone' = [clone EXCEPT !.phase = "aborted"]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

\* build_clone_request rereads the selected location after navigation returns.
CloneReadSnapshot ==
    /\ clone.phase = "readSnapshot"
    /\ clone' = [clone EXCEPT !.phase = IF ReadableByClone(clone.selected)
                                      THEN "selected" ELSE "aborted",
          !.cachedSelection = @ \/ (CloneCacheHits /\ clone.selected \notin store.snapshots)]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

\* CREATE retries a source-version mismatch; the chosen SNAPSHOT ID stays fixed.
CloneConflict == [clone EXCEPT
    !.phase = IF clone.refreshes < MaxCloneRefreshes THEN "refresh" ELSE "aborted",
    !.refreshes = IF clone.refreshes < MaxCloneRefreshes THEN @ + 1 ELSE @]

ClonePrecheck ==
    /\ clone.phase = "selected"
    /\ clone' = IF clone.sourceSeq # source.seq THEN CloneConflict
                ELSE [clone EXCEPT !.phase = IF Time(clone.selected) < source.lvt
                      THEN "aborted" ELSE "checked", !.fenceSeq = source.lvtSeq]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

CloneStage ==
    /\ clone.phase = "checked"
    /\ IF clone.sourceSeq = source.seq /\ clone.fenceSeq = source.lvtSeq
       THEN /\ clone' = [clone EXCEPT !.phase = "staged", !.bound = TRUE,
                  !.head = clone.selected, !.lvt = source.lvt,
                  !.firstDuringWrite = (~source.grouped
                     /\ writer.phase \notin {"idle", "committed", "aborted"})]
            /\ source' = [source EXCEPT !.grouped = TRUE,
                  !.seq = IF source.grouped THEN @ ELSE @ + 1,
                  !.lvtSeq = IF source.grouped THEN @ ELSE @ + 1]
       ELSE /\ clone' = IF clone.sourceSeq # source.seq THEN CloneConflict
                         ELSE [clone EXCEPT !.phase = "selected"]
            /\ UNCHANGED source
    /\ UNCHANGED <<store, writer, gc, clock, advanced, pendingFirst>>

\* Anchor uses current wall time, NO source predecessor, and the saved source
\* snapshot's segments. Its timestamp does NOT date the inherited data.
CloneWriteAnchor ==
    /\ clone.phase = "staged" /\ ~clone.failed
    /\ store' = [store EXCEPT !.snapshots = @ \cup {"anchor"},
                    !.times["anchor"] = clock, !.refs["anchor"] = Refs(clone.selected)]
    /\ clone' = [clone EXCEPT !.phase = "anchorWritten"]
    /\ UNCHANGED <<source, writer, gc, clock, advanced, pendingFirst>>

CloneInstallAnchor ==
    /\ clone.phase = "anchorWritten" /\ ~clone.failed
    /\ clone' = [clone EXCEPT !.phase = "installed", !.head = "anchor"]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

ClonePublish ==
    /\ clone.phase = "installed" /\ ~clone.failed
    /\ clone' = [clone EXCEPT !.phase = "published"]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

\* Fail before writing, after writing, or after metadata installation. Leave
\* the staged binding and its current head in place, as the interpreter does.
CloneFail ==
    /\ clone.phase \in {"staged", "anchorWritten", "installed"}
    /\ ~clone.failed
    /\ clone' = [clone EXCEPT !.failed = TRUE]
    /\ UNCHANGED <<source, store, writer, gc, clock, advanced, pendingFirst>>

GcObserveFence ==
    /\ gc.phase = "idle"
    /\ gc' = [gc EXCEPT !.phase = "owner", !.fenceSeq = source.lvtSeq]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcReadOwner ==
    /\ gc.phase = "owner"
    /\ gc' = [gc EXCEPT !.phase = "select", !.root = source.head]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

\* Count retention truncates the directory to len - keep + 2; it does NOT
\* walk committed ancestry. The captured owner head is included in the listing.
GcListSnapshots ==
    /\ gc.phase = "select"
    /\ LET listed == {s \in store.snapshots \cap SourceSnaps :
                         s = gc.root \/ KeyBefore(s, gc.root)}
           count == Cardinality(listed)
           prefix == {s \in listed : Cardinality({p \in listed : KeyBefore(p, s)})
                                     < count - RetainSnapshots + 2}
       IN IF gc.root \notin store.snapshots \/ (RetainSnapshots > 1 /\ count <= RetainSnapshots)
          THEN gc' = [gc EXCEPT !.phase = "deferred"]
          ELSE IF RetainSnapshots = 1
               THEN gc' = [gc EXCEPT !.phase = "readRoot", !.listedSnaps = listed]
               ELSE \E a \in prefix:
                    /\ \A s \in prefix : s = a \/ KeyBefore(s, a)
                    /\ gc' = [gc EXCEPT !.phase = "readAnchor", !.anchor = a,
                                         !.listedSnaps = prefix]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcReadAnchor ==
    /\ gc.phase = "readAnchor"
    /\ IF gc.anchor \notin store.snapshots \/ store.prev[gc.anchor] = "none"
       THEN gc' = [gc EXCEPT !.phase = "deferred"]
       ELSE gc' = [gc EXCEPT !.phase = "readRoot", !.root = store.prev[gc.anchor]]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcSelectRoot ==
    /\ gc.phase = "readRoot"
    /\ IF gc.root \notin store.snapshots \cap gc.listedSnaps
       THEN gc' = [gc EXCEPT !.phase = "deferred"]
       ELSE gc' = [gc EXCEPT !.phase = "listClones", !.cutoff = Time(gc.root),
              !.protectedSegs = Refs(gc.root),
              !.snapCandidates = {s \in gc.listedSnaps : KeyBefore(s, gc.root)}]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcListClones ==
    /\ gc.phase = "listClones"
    /\ gc' = [gc EXCEPT !.phase = IF clone.bound THEN "readClone" ELSE "marked",
                 !.sawClone = clone.bound]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcReadClone ==
    /\ gc.phase = "readClone"
    /\ gc' = [gc EXCEPT !.phase = "loadClone", !.readHead = clone.head,
                 !.cloneFenceSeq = clone.lvtSeq]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcLoadClone ==
    /\ gc.phase = "loadClone"
    /\ IF gc.readHead \notin store.snapshots
       THEN gc' = [gc EXCEPT !.phase = "deferred"]
       ELSE gc' = [gc EXCEPT !.phase = "marked",
              !.protectedSegs = @ \cup Refs(gc.readHead),
              !.protectedSnaps = IF PinStagedSnapshot /\ gc.readHead # "anchor"
                                THEN @ \cup {gc.readHead} ELSE @,
              !.touchClone = (gc.readHead = "anchor"),
              !.cloneFenceTime = Max(clone.lvt, Time(gc.readHead))]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcPublishFence ==
    /\ gc.phase = "marked"
    /\ IF gc.fenceSeq = source.lvtSeq
          /\ (~gc.touchClone \/ gc.cloneFenceSeq = clone.lvtSeq)
       THEN /\ source' = [source EXCEPT !.lvt = Max(@, gc.cutoff), !.lvtSeq = @ + 1]
            /\ clone' = IF gc.touchClone
                        THEN [clone EXCEPT !.lvt = gc.cloneFenceTime, !.lvtSeq = @ + 1]
                        ELSE clone
            /\ gc' = [gc EXCEPT !.phase = "rescan"]
       ELSE /\ gc' = [gc EXCEPT !.phase = "deferred"]
            /\ UNCHANGED <<source, clone>>
    /\ UNCHANGED <<store, writer, clock, advanced, pendingFirst>>

\* Rescan only IDs absent from the FIRST binding listing, not all descendants.
GcRescan ==
    /\ gc.phase = "rescan"
    /\ gc' = [gc EXCEPT !.phase = IF RescanClones /\ ~gc.sawClone /\ clone.bound
                        THEN "readLate" ELSE "listSegments"]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcReadLate ==
    /\ gc.phase = "readLate"
    /\ gc' = [gc EXCEPT !.phase = "loadLate", !.readHead = clone.head]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcLoadLate ==
    /\ gc.phase = "loadLate"
    /\ IF gc.readHead \notin store.snapshots
       THEN gc' = [gc EXCEPT !.phase = "deferred"]
       ELSE gc' = [gc EXCEPT !.phase = "listSegments",
              !.protectedSegs = @ \cup Refs(gc.readHead),
              !.protectedSnaps = IF PinStagedSnapshot /\ gc.readHead # "anchor"
                                THEN @ \cup {gc.readHead} ELSE @]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcListSegments ==
    /\ gc.phase = "listSegments"
    /\ gc' = [gc EXCEPT !.phase = "loadSegments",
          !.segCandidates = {g \in store.segments : store.born[g] < gc.cutoff} \ gc.protectedSegs]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcLoadSegments ==
    /\ gc.phase = "loadSegments"
    /\ gc' = [gc EXCEPT !.phase = IF gc.protectedSegs \subseteq store.segments
                    THEN "listBlocks" ELSE "deferred",
                 !.protectedBlocks = BlocksOf(gc.protectedSegs)]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

GcListBlocks ==
    /\ gc.phase = "listBlocks"
    /\ gc' = [gc EXCEPT !.phase = "blocks",
          !.blockCandidates = {b \in store.blocks : store.born[b] < gc.cutoff} \ gc.protectedBlocks]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

\* Preserve the implementation order: blocks, segments, then snapshots.
\* Listings are frozen; newly written files are not retroactively listed.
GcDeleteBlock ==
    /\ gc.phase = "blocks"
    /\ \E b \in gc.blockCandidates:
          /\ store' = [store EXCEPT !.blocks = @ \ {b}]
          /\ gc' = [gc EXCEPT !.blockCandidates = @ \ {b}]
    /\ UNCHANGED <<source, writer, clone, clock, advanced, pendingFirst>>
GcBlocksDone ==
    /\ gc.phase = "blocks" /\ gc.blockCandidates = {}
    /\ gc' = [gc EXCEPT !.phase = "segments"]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>
GcDeleteSegment ==
    /\ gc.phase = "segments"
    /\ \E g \in gc.segCandidates:
          /\ store' = [store EXCEPT !.segments = @ \ {g}]
          /\ gc' = [gc EXCEPT !.segCandidates = @ \ {g}]
    /\ UNCHANGED <<source, writer, clone, clock, advanced, pendingFirst>>
GcSegmentsDone ==
    /\ gc.phase = "segments" /\ gc.segCandidates = {}
    /\ gc' = [gc EXCEPT !.phase = "snapshots",
                 !.snapCandidates = @ \ gc.protectedSnaps]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>
GcDeleteSnapshot ==
    /\ gc.phase = "snapshots"
    /\ \E s \in gc.snapCandidates:
          /\ store' = [store EXCEPT !.snapshots = @ \ {s}]
          /\ gc' = [gc EXCEPT !.snapCandidates = @ \ {s}]
    /\ UNCHANGED <<source, writer, clone, clock, advanced, pendingFirst>>
GcFinish ==
    /\ gc.phase = "snapshots" /\ gc.snapCandidates = {}
    /\ gc' = [gc EXCEPT !.phase = "done"]
    /\ UNCHANGED <<source, store, writer, clone, clock, advanced, pendingFirst>>

Next == WriterBegin \/ WriterSampleTime \/ WriterWriteBlock \/ WriterWriteSegment
        \/ WriterWriteSnapshot \/ WriterBuffer
        \/ WriterNextStatement \/ WriterLoadDiff \/ WriterPrecheck \/ WriterCAS
        \/ WriterRetryRead \/ WriterRetryTime \/ WriterRebuild \/ AdvanceSource
        \/ AdvanceRead \/ AdvanceSampleTime \/ AdvanceWriteBlock \/ AdvanceWriteSegment
        \/ AdvanceWriteSnapshot \/ AdvancePrecheck \/ AdvanceCAS
        \/ CloneReadSource \/ CloneNavigate \/ CloneReadSnapshot \/ ClonePrecheck \/ CloneStage \/ CloneWriteAnchor
        \/ CloneInstallAnchor \/ ClonePublish \/ CloneFail
        \/ GcObserveFence \/ GcReadOwner \/ GcListSnapshots \/ GcReadAnchor \/ GcSelectRoot \/ GcListClones
        \/ GcReadClone \/ GcLoadClone \/ GcPublishFence \/ GcRescan
        \/ GcReadLate \/ GcLoadLate \/ GcListSegments \/ GcLoadSegments \/ GcListBlocks
        \/ GcDeleteBlock \/ GcBlocksDone \/ GcDeleteSegment \/ GcSegmentsDone
        \/ GcDeleteSnapshot \/ GcFinish

Roots == {source.head} \cup (IF clone.bound THEN {clone.head} ELSE {})
PublishedSnapshotsExist == Roots \subseteq store.snapshots
PublishedSegmentsExist == \A s \in Roots : Refs(s) \subseteq store.segments
PublishedBlocksExist == \A s \in Roots : BlocksOf(Refs(s)) \subseteq store.blocks
PublishedReferencesExist == PublishedSnapshotsExist /\ PublishedSegmentsExist /\ PublishedBlocksExist
FirstCloneReferencesExist == clone.firstDuringWrite => PublishedReferencesExist
BufferedFenceIsMinimum ==
    (writer.count > 0 /\ writer.viewGrouped) => writer.fence = Time("w1")
OriginalObjectTimePreserved == writer.count > 0 => writer.begin = store.born["b1"]

TypeOK ==
    /\ source.head \in SourceSnaps /\ source.seq \in 0..3
    /\ source.grouped \in BOOLEAN /\ source.lvt \in 0..20 /\ source.lvtSeq \in 0..2
    /\ store.snapshots \subseteq Snaps /\ store.segments \subseteq Segs
    /\ store.blocks \subseteq Blocks /\ store.times \in [Snaps -> 0..20]
    /\ store.refs \in [Snaps -> SUBSET Segs] /\ store.born \in [Segs \cup Blocks -> 0..22]
    /\ store.prev \in [Snaps -> Snaps \cup {"none"}]
    /\ writer.phase \in {"idle", "statement", "writeBlock", "writeSegment", "writeSnapshot", "buffer", "buffered",
                          "ready", "checked", "retry", "retryTime", "rebuild", "committed", "aborted"}
    /\ writer.count \in 0..Statements /\ writer.retries \in 0..MaxRetries
    /\ writer.pending \in Snaps /\ writer.base \in SourceSnaps
    /\ writer.latest \in SourceSnaps /\ writer.cache \subseteq Snaps
    /\ writer.seq \in 0..3 /\ writer.fenceSeq \in 0..2 /\ writer.viewGrouped \in BOOLEAN
    /\ writer.begin \in 0..22 /\ writer.fence \in (0..20) \cup {NoFence}
    /\ writer.statementTime \in 0..20 /\ writer.blockTime \in 0..22
    /\ writer.retryTime \in 0..20 /\ writer.retryBlockTime \in 0..22
    /\ clone.phase \in {"idle", "refresh", "navigate", "readSnapshot", "selected", "checked", "aborted", "staged",
                         "anchorWritten", "installed", "published"}
    /\ clone.selected \in SourceSnaps /\ clone.head \in Snaps
    /\ clone.cursor \in SourceSnaps /\ clone.cachedSelection \in BOOLEAN
    /\ clone.target \in SourceSnaps \cup {"choose"} /\ clone.refreshes \in 0..MaxCloneRefreshes
    /\ clone.bound \in BOOLEAN /\ clone.failed \in BOOLEAN
    /\ clone.firstDuringWrite \in BOOLEAN
    /\ clone.sourceSeq \in 0..3 /\ clone.fenceSeq \in 0..2
    /\ clone.lvt \in 0..20 /\ clone.lvtSeq \in 0..1
    /\ gc.phase \in {"idle", "owner", "select", "readAnchor", "readRoot", "listClones", "readClone", "loadClone",
          "marked", "rescan", "readLate", "loadLate", "listSegments", "loadSegments",
          "listBlocks", "blocks", "segments", "snapshots", "done", "deferred"}
    /\ gc.root \in SourceSnaps /\ gc.readHead \in Snaps /\ gc.cutoff \in 0..20
    /\ gc.anchor \in SourceSnaps /\ gc.listedSnaps \subseteq SourceSnaps
    /\ gc.protectedSegs \subseteq Segs /\ gc.protectedSnaps \subseteq Snaps
    /\ gc.protectedBlocks \subseteq Blocks /\ gc.snapCandidates \subseteq SourceSnaps
    /\ gc.segCandidates \subseteq Segs /\ gc.blockCandidates \subseteq Blocks
    /\ gc.fenceSeq \in 0..2 /\ gc.cloneFenceSeq \in 0..1 /\ gc.cloneFenceTime \in 0..20
    /\ gc.sawClone \in BOOLEAN /\ gc.touchClone \in BOOLEAN
    /\ clock \in {1, 3} /\ pendingFirst \in BOOLEAN
    /\ advanced.phase \in {"idle", "sample", "writeBlock", "writeSegment",
          "writeSnapshot", "ready", "checked", "committed", "conflict", "aborted"}
    /\ advanced.base \in SourceSnaps /\ advanced.seq \in 0..3
    /\ advanced.time \in 0..20 /\ advanced.grouped \in BOOLEAN
    /\ advanced.fenceSeq \in 0..2

NeverFirstCloneAbort ==
    ~(writer.phase = "aborted" /\ clone.firstDuringWrite /\ gc.phase = "done"
      /\ writer.count > 0 /\ writer.begin < gc.cutoff)
NeverFailedStagedCloneProtected ==
    ~(gc.phase = "done" /\ clone.failed /\ clone.head # "anchor"
      /\ Time(clone.head) < gc.cutoff)
NeverAnchorInstalledDuringMark ==
    ~(gc.phase = "done" /\ gc.readHead # "anchor" /\ clone.head = "anchor"
      /\ clone.phase = "published" /\ clone.selected \in gc.protectedSnaps
      /\ Time(clone.selected) < gc.cutoff)
NeverOffsetRetryCommit ==
    ~(writer.phase = "committed" /\ writer.retries > 0 /\ Delta > 0
      /\ writer.begin > Time("w1") /\ gc.phase = "done"
      /\ "b0" \notin store.blocks)
NeverCachedCloneRejected ==
    ~(clone.phase = "aborted" /\ clone.cachedSelection /\ gc.phase = "done")
TransactionHistorySkipsBufferedSnapshots ==
    Time("w2") > 0 => store.prev["w2"] = writer.base
NeverTransactionalHistoryCollapsed ==
    ~(Statements = 2 /\ writer.phase = "committed" /\ source.head = "w2"
      /\ "w1" \in store.snapshots /\ store.prev["w2"] = writer.base)
NeverCloneRefreshPublished ==
    ~(clone.refreshes > 0 /\ clone.phase = "published" /\ gc.phase = "done")
NeverAdvancePartialGc ==
    ~(SplitAdvance /\ advanced.phase = "writeSegment" /\ gc.phase = "done"
      /\ store.born["bx"] < gc.cutoff /\ "bx" \notin store.blocks)
NeverAdvanceConflictAfterGc ==
    ~(SplitAdvance /\ advanced.phase = "conflict" /\ gc.phase = "done"
      /\ Time("sx") > 0 /\ "bx" \notin store.blocks)
NeverUnpublishedAdvanceSelected ==
    ~(SplitAdvance /\ gc.phase = "readAnchor" /\ gc.anchor = "sx"
      /\ advanced.phase \in {"ready", "checked", "conflict"})
NeverWriterFenceRace ==
    ~(writer.phase = "checked" /\ writer.seq = source.seq
      /\ writer.viewGrouped /\ writer.fenceSeq # source.lvtSeq)
NeverFirstCloneInvalidatesMark ==
    ~(~InitiallyGrouped /\ gc.phase = "marked" /\ source.grouped
      /\ gc.fenceSeq # source.lvtSeq /\ source.lvt = 0)
=============================================================================
