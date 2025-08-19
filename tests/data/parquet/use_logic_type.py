import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, date

data = {
    "timestamp_ms": [
        datetime(2023, 1, 1, 12, 0, 0),
        datetime(2023, 1, 2, 12, 30, 15),
        datetime(2023, 1, 3, 18, 45, 30),
    ],
    "timestamp_us": [
        datetime(2023, 1, 1, 12, 0, 0, 123456),
        datetime(2023, 1, 2, 12, 30, 15, 789012),
        datetime(2023, 1, 3, 18, 45, 30, 999999),
    ],
    "timestamp_ns": [
        pd.Timestamp("2023-01-01 12:00:00.123456789"),
        pd.Timestamp("2023-01-02 12:30:15.987654321"),
        pd.Timestamp("2023-01-03 18:45:30.999999999"),
    ],
    "date32": [date(2023, 1, 1), date(2023, 1, 15), date(2023, 2, 28)],
}

df = pd.DataFrame(
    {
        "timestamp_ms": data["timestamp_ms"],
        "timestamp_us": data["timestamp_us"],
        "timestamp_ns": data["timestamp_ns"],
        "date32": pd.to_datetime(data["date32"]).date,
    }
)

schema = pa.schema(
    [
        pa.field("timestamp_ms", pa.timestamp("ms")),
        pa.field("timestamp_us", pa.timestamp("us")),
        pa.field("timestamp_ns", pa.timestamp("ns")),
        pa.field("date32", pa.date32()),
    ]
)

table = pa.Table.from_pandas(df, schema=schema)

pq.write_table(table, "timestamp_types_example.parquet", version="2.6")
