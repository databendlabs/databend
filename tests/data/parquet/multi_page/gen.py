import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

row_group_size = 20
names = [1, 3, 2, 4]  # small, large, small, large
for i in range(4):
    num_row = row_group_size * (i + 1)
    # to test https://github.com/datafuselabs/databend/pull/11271
    # we need multi pages in a column chunk for list type
    col_arr = [[1], [1, 2]] * num_row
    col_int = [0, 1] * num_row

    df = pd.DataFrame({"col_arr": col_arr, "col_int": col_int})

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(
        table,
        f"multi_page_{names[i]}.parquet",
        write_batch_size=1,
        data_page_size=128,
        row_group_size=row_group_size,
    )
