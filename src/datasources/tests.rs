// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_datasource() -> crate::error::FuseQueryResult<()> {
    use std::sync::Arc;

    use crate::datasources::*;
    use crate::datavalues::*;
    use crate::testdata;

    let csv_test_source = testdata::CsvTestData::create();
    let mut mem_database = Database::create("mem_db");
    let mem_table = MemoryTable::create(
        "mem_table",
        Arc::new(DataSchema::new(vec![DataField::new(
            "a",
            DataType::Int64,
            false,
        )])),
    );
    mem_database.add_table(Arc::new(mem_table))?;

    let mut csv_database = Database::create("csv_db");
    let csv_table = CsvTable::create(
        "csv_table",
        11,
        csv_test_source.csv_table_schema_for_test(),
        csv_test_source.csv_table_partitions_for_test(),
    );
    csv_database.add_table(Arc::new(csv_table))?;

    let mut datasource = DataSource::create();
    datasource.add_database(Arc::new(mem_database))?;
    datasource.add_database(Arc::new(csv_database))?;

    let table = datasource.get_table("mem_db", "mem_table")?;
    assert_eq!("mem_table", table.name());

    let table = datasource.get_table("csv_db", "csv_table")?;
    assert_eq!("csv_table", table.name());

    let table = datasource.get_table("not_found_db", "not_found_table");
    match table {
        Ok(_) => {}
        Err(e) => assert_eq!(
            "Internal Error: Can not find the database: not_found_db",
            e.to_string()
        ),
    }

    let table = datasource.get_table("csv_db", "not_found_table");
    match table {
        Ok(_) => {}
        Err(e) => assert_eq!(
            "Internal Error: Can not find the table: not_found_table",
            e.to_string()
        ),
    }

    Ok(())
}
