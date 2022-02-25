(WIP)

situations:

  - format changed, e.g. json -> bson/bcode/customized binary...
  - add / remove fields
  - co-existence of multiple formats


principles:
  - backward compatible 


loading:
    1. Get TableInfo from meta server
       from `TableInfo`, we know the `format-version` of the snapshot.
    2. using the `format-version`, get the `reader` from the `MetaReaderFactory`


~~~

let snapshot_format_version = tbl_info.snaphost_version();

let snapshot_reader = MetaReaders.snapshot_reader(snapshot_format_version)?;
let snapshot = snapshot_reader.read(snaphost_location).await?;
for segment_meta in snaphost.segments {
    let seg_ver = segment_meta.format_version();
    let seg_reader = MetaReaders.seg_reader(seg_ver);
    let segment = seg_reader.read(segment_meta.location);
}

~~~
    
   
