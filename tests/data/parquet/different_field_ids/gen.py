from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


output_dir = Path(__file__).parent

for file_name, field_id, values in [
    ("f1.parquet", 1, [1, 2]),
    ("f2.parquet", 2, [3, 4]),
]:
    field = pa.field(
        "id",
        pa.int32(),
        nullable=False,
        metadata={b"PARQUET:field_id": str(field_id).encode()},
    )
    table = pa.Table.from_arrays(
        [pa.array(values, type=pa.int32())], schema=pa.schema([field])
    )
    pq.write_table(table, output_dir / file_name)
