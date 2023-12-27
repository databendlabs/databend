import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


df = pd.DataFrame({"c1": range(1, 11), "c2": np.int8(range(2, 12)), "c3": range(3, 13)})
table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_table(table, f"c1c2c3.parquet", row_group_size=3)

df = pd.DataFrame({"c2": range(12, 22), "c3": range(13, 23), "c4": map(str, range(14, 24))})
table = pa.Table.from_pandas(df, preserve_index=False)
pq.write_table( table, f"c2c3c4.parquet", row_group_size=3)
