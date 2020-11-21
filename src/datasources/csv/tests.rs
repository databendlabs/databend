// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_csv_table() -> crate::error::FuseQueryResult<()> {
    use crate::datasources::*;
    use crate::testdata;
    use tokio::stream::StreamExt;

    let csv_test_source = testdata::CsvTestData::create();

    let csv = CsvTable::create(
        "csv_table",
        11,
        csv_test_source.csv_table_schema_for_test(),
        csv_test_source.csv_table_partitions_for_test(),
    );
    let mut stream = csv
        .read(csv_test_source.csv_table_partitions_for_test())
        .await?;

    let mut rows = 0;
    while let Some(v) = stream.next().await {
        let row = v?.num_rows();
        rows += row;
    }
    assert_eq!(201, rows);
    Ok(())
}
