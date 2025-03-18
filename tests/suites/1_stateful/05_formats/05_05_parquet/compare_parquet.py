import pandas as pd

df1 = pd.read_parquet('../unload2/data_35d16e1f-0155-4463-8afe-924786b45c28_0000_00000000.parquet')
df2 = pd.read_parquet('./data_b6be89dd-ba81-4e98-b6ee-41a149034e18_0000_00000000.parquet')

print(df1.equals(df2))
