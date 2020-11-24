// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datasources::*;
    use crate::datavalues::*;
    use crate::testdata;

    let csvtestdata = testdata::CsvTestData::create();
    let datasource = csvtestdata.csv_table_datasource_for_test();
    datasource.lock()?.add_database("mem_db")?;
    let mem_table = MemoryTable::create(
        "mem_table",
        Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Int64,
            false,
        )])),
    );
    datasource
        .lock()
        .unwrap()
        .add_table("mem_db", Arc::new(mem_table))?;

    datasource.lock()?.add_database("csv_db")?;
    let csv_table = CsvTable::create(
        "csv_table",
        11,
        csvtestdata.csv_table_schema_for_test(),
        csvtestdata.csv_table_partitions_for_test(),
    );
    datasource
        .lock()
        .unwrap()
        .add_table("csv_db", Arc::new(csv_table))?;

    let table = datasource.lock()?.get_table("mem_db", "mem_table")?;
    assert_eq!("mem_table", table.name());

    let table = datasource.lock()?.get_table("csv_db", "csv_table")?;
    assert_eq!("csv_table", table.name());

    let table = datasource
        .lock()?
        .get_table("not_found_db", "not_found_table");
    match table {
        Ok(_) => {}
        Err(e) => assert_eq!(
            "Internal Error: Can not find the database: not_found_db",
            e.to_string()
        ),
    }

    let table = datasource.lock()?.get_table("csv_db", "not_found_table");
    match table {
        Ok(_) => {}
        Err(e) => assert_eq!(
            "Internal Error: Can not find the table: not_found_table",
            e.to_string()
        ),
    }

    Ok(())
}
