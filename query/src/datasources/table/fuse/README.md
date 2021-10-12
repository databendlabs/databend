

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
  
  For each block, put it in object storage (as parquet for the time being).  
    
  Segment are generated for those blocks, which tracks all the block
  meta information. also, statistics of each block are aggregated and kept 
  int the segments.

  Segments are stored in object storage as well.
 
     
- commit (by "Driver" role)

  Gather all the segments(info) , aggregates the statistics, merge segments
  with previous snapshot, and commit.  

  In case of conflicts, "Driver" need to re-try the transaction.(OCC, Table level, READ-COMMITTED)

  For this iteration, the "Driver" is the `Table` itself.


**Scan Flow:**


- `Table::read_plan`

   Prunes bocks by using the scan expressions / criteria, and statistics in Snapshot / Segment.

- `Table::read`

  Prunes columns/roles by using the plan criteria, and statistics/index insides the parquet file.

