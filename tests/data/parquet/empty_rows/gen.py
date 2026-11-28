import pyarrow as pa
import pyarrow.parquet as pq

# A valid Parquet file with no rows, as some writers produce for an empty partition.
table = pa.table({"k": pa.array([], pa.int32()), "v": pa.array([], pa.int32())})
pq.write_table(table, "empty.parquet")
