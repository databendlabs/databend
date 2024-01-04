import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

df = pd.DataFrame(
    {"c1": range(110, 120), "c2": np.int16(range(120, 130)), "c3": range(130, 140)}
)
table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_table(table, f"f1.parquet", row_group_size=3)

df = pd.DataFrame(
    {
        "c2": range(220, 230),
        "c4": map(str, range(240, 250)),
        "c5": range(250, 260),
        "c6": range(260, 270),
    }
)

df = df[["c6", "c5", "c2", "c4"]]
table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_table(table, f"f2.parquet", row_group_size=3)
