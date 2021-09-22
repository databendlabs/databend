

NOTE:

This is an ongoing work.

**Table Layout**

A table comprised of a list of snapshots. MetaStore keeps a pointer to 
the latest snapshot of a given table.

- Snapshot

  A consist view of given table, which comprises
 
  - pointers to `Segment`s
  - Table level aggregated statistics
  - pointer to previous snapshot
   
- Segment
 
  An intermediate level meta information, which comprises 
 
  - pointers to `Block`s
  - Segment level aggregated statistics
   
- Block
 
  The basic unit of data for a table.

**Ingestion Flow:**

- Insert `Interpreter`

  Accumulates/batch data into blocks, naturally ordered, not partitioning
t this stage, we reply on background task to merge the data properly.
  
- `Table::append`
  
  For each block, save it in object store (as parquet for the time being).  
    
  A segment info is generated for those blocks, which tracks all the block
  meta information. also, statistics of each block are aggregated and kept 
  int the segment info.
 
  Save Segment info in object store.
 
  NOTE: 
    - `append` may be executed parallel.
    - operations should be logged/journaled in case of rollback/abort 
     
    - "Driver"

      Gather all the segments(info) , aggregates the statistics, merge segments
      with previous snapshot, and commit.  

      In case of conflicts, "Driver" need to re-try the transaction.(OCC, Table level, READ-COMMITTED)

      For this iteration, the "Driver" is `Table` itself.


**Scan Flow:**


- `Table::read_plan`

   Prunes bocks by using the scan expressions / criteria, and statistics in Snapshot / Segment.

- `Table::append`

  Prunes columns by using the plan criteria 

