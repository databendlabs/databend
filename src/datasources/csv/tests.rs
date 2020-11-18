// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[async_std::test]
async fn test_csv_table() -> crate::error::Result<()> {
    use crate::datasources::*;
    use crate::testdata;
    use async_std::stream::StreamExt;

    let csv = CsvTable::create(
        "csv_table",
        11,
        testdata::test_data_csv_schema(),
        testdata::test_data_csv_partitions(),
    );
    let mut stream = csv.read(testdata::test_data_csv_partitions()).await?;

    let mut rows = 0;
    while let Some(v) = stream.next().await {
        let row = v?.num_rows();
        rows += row;
    }
    assert_eq!(201, rows);
    Ok(())
}
