import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

names = ["s", "ms", "us", "ns"]  # small, large, small, large
for i in range(len(names)):
    num_row = 300
    timestamps = [
        "2023-10-13T10:00:00",
        "2023-10-14T11:00:00",
        "2023-10-15T12:00:00",
        "2023-10-16T12:00:00",
    ] * num_row
    datetime_objects = [datetime.fromisoformat(timestamp) for timestamp in timestamps]
    timestamp_array = pa.array(datetime_objects, type=pa.timestamp(f"{names[i]}"))

    table = pa.Table.from_arrays([timestamp_array], ["col_timestamp"])

    pq.write_table(
        table,
        f"timestamp_{names[i]}.parquet",
        row_group_size=num_row / 2,
    )
